from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CDRTeleparserSchema:
    name: str
    column_mapping: list[tuple[str, str]]
    job_description: str

    def __post_init__(self) -> None:
        if not self.column_mapping:
            raise ValueError(
                f"Schema '{self.name}': column_mapping nao pode ser vazio."
            )
        for item in self.column_mapping:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not isinstance(item[1], str)
            ):
                raise ValueError(
                    f"Schema '{self.name}': cada item de column_mapping deve ser "
                    f"uma tupla (origem, destino) de strings. Recebido: {item!r}"
                )


class CDRTeleparserExtractor:
    _SCHEMAS: dict[str, CDRTeleparserSchema] = {
        "ericsson": CDRTeleparserSchema(
            name="Ericsson",
            column_mapping=[
                ("networkCallReference", "referencia"),
                ("callingPartyNumber.digits", "numero_origem"),
                ("dateForStartOfCharge", "_data"),
                ("timeForStartOfCharge", "_hora"),
                ("CallModule", "tipo_chamada"),
                ("calledPartyNumber.digits", "numero_destino"),
                ("chargeableDuration", "duracao"),
            ],
            job_description="Extraindo CDR Parquet: Ericsson",
        ),
        "tim_huawei": CDRTeleparserSchema(
            name="TIM Huawei",
            column_mapping=[
                ("network-Call-Reference", "referencia"),
                ("calling-Party-Address-Generic", "_numero_origem_generico"),
                ("list-Of-Calling-Party-Address_tEL-URI", "numero_origem"),
                ("recordOpeningTime", "data_hora"),
                ("role-of-Node", "tipo_chamada"),
                ("called-Party-Address_tEL-URI", "numero_destino"),
                ("duration", "duracao"),
                ("recordType", "_tipo_cdr"),
            ],
            job_description="Extraindo CDR Parquet: TIM Huawei",
        ),
        "vivo_fcdr": CDRTeleparserSchema(
            name="Vivo FCDR",
            column_mapping=[
                ("callModule", "_tipo_chamada"),
                ("callingPartyNumber", "_numero_origem"),
                ("calledPartyNumber", "numero_destino"),
                ("chargeableDurat", "duracao"),
                ("dateForStartOfCharge", "_data"),
                ("timeForStartOfCharge", "_hora"),
                ("networkCallReference", "referencia"),
            ],
            job_description="Extraindo CDR Parquet: Vivo FCDR",
        ),
        "nokia": CDRTeleparserSchema(
            name="Nokia",
            column_mapping=[
                ("record_type", "_tipo_chamada"),
                ("call_reference", "referencia"),
                ("call_reference_time", "data_hora_referencia"),
                ("in_channel_allocated_time", "data_hora_alocacao_canal"),
                ("calling_number", "numero_origem"),
                ("orig_calling_number", "numero_origem_original"),
                ("called_number", "numero_destino"),
                ("orig_called_number", "numero_destino_original"),
                ("connected_to_number", "numero_conectado"),
                ("forwarding_number", "numero_origem_encaminhamento"),
                ("forwarded_to_number", "numero_destino_encaminhamento"),
                ("orig_mcz_duration", "_duracao_orig_mcz"),
                ("term_mcz_duration", "_duracao_term_mcz"),
                ("forw_mcz_duration", "_duracao_forw_mcz"),
                ("roam_mcz_duration", "_duracao_roam_mcz"),
                ("iaz_duration", "_duracao_iaz"),
                ("oaz_duration", "_duracao_oaz"),
                ("chargeable_duration", "_duracao_tarifavel"),
                ("char_band_duration", "_duracao_banda_tarifavel"),
            ],
            job_description="Extraindo CDR Parquet: Nokia",
        ),
    }

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        # self._sc = spark.sparkContext

    def _extract_cdr(
        self,
        source_file: str,
        target_file: str,
        schema: CDRTeleparserSchema,
        ignore_missing_columns: bool = False,
        unique: bool = False,
    ) -> DataFrame:
        # self._sc.setJobDescription(schema.job_description)

        logger.info("Lendo arquivo parquet: %s", source_file)
        df = self.spark.read.parquet(source_file)

        logger.info(
            "Parâmetro ignore_missing_columns: %s. %s colunas ausentes.",
            ignore_missing_columns,
            "Ignorando" if ignore_missing_columns else "Verificando",
        )

        missing_columns = [
            source_col
            for source_col, _ in schema.column_mapping
            if source_col not in df.columns
        ]

        if missing_columns and not ignore_missing_columns:
            raise ValueError(
                f"Schema '{schema.name}' requer colunas ausentes no parquet: "
                f"{missing_columns}. Colunas disponiveis: {df.columns}"
            )

        if missing_columns and ignore_missing_columns:
            logger.warning(
                "Colunas ausentes no parquet: %s. Selecionando apenas colunas presentes.",
                missing_columns,
            )

        # Filtrar o mapeamento para incluir apenas colunas presentes quando ignore_missing_columns=True
        filtered_column_mapping = (
            [
                (source_col, target_col)
                for source_col, target_col in schema.column_mapping
                if source_col in df.columns
            ]
            if ignore_missing_columns
            else schema.column_mapping
        )

        select_expr = [
            F.col(f"`{source_col}`").alias(target_col)
            if "." in source_col
            else F.col(source_col).alias(target_col)
            for source_col, target_col in filtered_column_mapping
        ]

        df = (
            df.select(*select_expr)
            .withColumn(
                "prestadora", F.element_at(F.split(F.input_file_name(), "/"), -3)
            )
            .withColumn("tipo_cdr", F.element_at(F.split(F.input_file_name(), "/"), -2))
            .withColumn(
                "arquivo_origem", F.element_at(F.split(F.input_file_name(), "/"), -1)
            )
        )

        if unique:
            df = df.dropDuplicates()
            logger.info(
                "Parâmetro unique: %s. Removendo duplicatas.",
                unique,
            )

        if "_tipo_cdr" in df.columns:
            df = df.withColumn("tipo_cdr", F.col("_tipo_cdr")).drop("_tipo_cdr")

        logger.info("Escrevendo DataFrame extraido para parquet: %s", target_file)
        df.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

    @log_operation
    def extract_cdr_tim_huawei(self, source_file: str, target_file: str) -> DataFrame:
        df = self._extract_cdr(
            source_file, target_file, self._SCHEMAS["tim_huawei"], unique=True
        )
        return df

    @log_operation
    def extract_cdr_vivo_fcdr(self, source_file: str, target_file: str) -> DataFrame:
        df = self._extract_cdr(source_file, target_file, self._SCHEMAS["vivo_fcdr"])
        return df

    @log_operation
    def extract_cdr_nokia(self, source_file: str, target_file: str) -> DataFrame:
        df = self._extract_cdr(
            source_file,
            target_file,
            self._SCHEMAS["nokia"],
            ignore_missing_columns=True,
        )
        return df
