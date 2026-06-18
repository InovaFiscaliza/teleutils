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
        "tim_ats": CDRTeleparserSchema(
            name="TIM ATS",
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
            job_description="Extraindo CDR Parquet: TIM ATS",
        ),
    }

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._sc = spark.sparkContext

    def _extract_cdr(
        self, source_file: str, target_file: str, schema: CDRTeleparserSchema
    ) -> DataFrame:
        self._sc.setJobDescription(schema.job_description)

        logger.info("Lendo arquivo parquet: %s", source_file)
        df = self.spark.read.parquet(source_file)

        missing_columns = [
            source_col
            for source_col, _ in schema.column_mapping
            if source_col not in df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Schema '{schema.name}' requer colunas ausentes no parquet: "
                f"{missing_columns}. Colunas disponiveis: {df.columns}"
            )

        select_expr = [
            F.col(f"`{source_col}`").alias(target_col)
            if "." in source_col
            else F.col(source_col).alias(target_col)
            for source_col, target_col in schema.column_mapping
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

        if "_tipo_cdr" in df.columns:
            df = df.withColumn("tipo_cdr", F.col("_tipo_cdr")).drop("_tipo_cdr")

        logger.info("Escrevendo DataFrame extraido para parquet: %s", target_file)
        df.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

    @log_operation
    def extract_tim_ats(self, source_file: str, target_file: str) -> DataFrame:
        df = self._extract_cdr(source_file, target_file, self._SCHEMAS["tim_ats"])
        return df
