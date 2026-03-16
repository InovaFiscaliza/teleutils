from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

from teleutils._logging import log_operation
from teleutils.preprocessing import normalize_number

logger = logging.getLogger(__name__)

_return_schema = T.StructType(
    [
        T.StructField("numero_formatado", T.StringType(), True),
        T.StructField("numero_valido", T.BooleanType(), True),
    ]
)


@pandas_udf(_return_schema)
def _spark_normalize_number(number_series: pd.Series) -> pd.DataFrame:
    # Processar em batch (vetorizado)
    results = [normalize_number(n) for n in number_series]

    return pd.DataFrame(
        results,
        columns=[
            "numero_formatado",
            "numero_valido",
        ],
    )


class RoboCallsTransformer:
    # Valores possíveis de chamada_autenticada:
    #  0 = não autenticada (é nulo)
    # +1 = autenticação válida (contém TN-Validation-Pa)
    # -1 = autenticação falhou (em qualquer outro caso)
    _AUTH_NONE = 0
    _AUTH_PASS = 1
    _AUTH_FAIL = -1

    _LIMIAR_CHAMADA_OFENSORA = 6

    _TRANSFORM_MAP: dict[str, str] = {
        "ericsson": "transform_cdr_ericsson",
        "tim_volte": "transform_cdr_tim_volte",
        "tim_stir": "transform_cdr_tim_stir",
        "vivo_volte": "transform_cdr_vivo_volte",
    }

    _TRANSFORMED_COLUMNS = [
        "referencia",
        "tipo_de_chamada",
        "data_hora",
        "numero_de_a_formatado",
        "numero_de_b_formatado",
        "hora_da_chamada",
        "duracao_da_chamada",
        "chamada_curta",
        "chamada_autenticada",
        "chamada_caixa_postal",
    ]

    def __init__(
        self,
        spark: SparkSession,
        limiar_chamada_ofensora: int = _LIMIAR_CHAMADA_OFENSORA,
    ):
        self.spark = spark
        self.limiar_chamada_ofensora = limiar_chamada_ofensora

    def _format_columns(self, df, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"):
        return (
            df.withColumn(
                "duracao_da_chamada",
                F.coalesce(F.col("duracao_da_chamada").cast(T.IntegerType()), F.lit(0)),
            )
            .withColumn(
                "data_hora",
                F.to_timestamp(
                    F.concat_ws(" ", F.col("_data"), F.col("_hora")), date_time_fmt
                ),
            )
            .withColumn(
                "hora_da_chamada", F.date_format(F.col("data_hora"), "yyyyMMddHH")
            )
        )

    def _format_numbers(self, df):
        return df.withColumn(
            "numero_de_a_formatado",
            _spark_normalize_number("numero_de_a").getField("numero_formatado"),
        ).withColumn(
            "numero_de_b_formatado",
            _spark_normalize_number("numero_de_b").getField("numero_formatado"),
        )

    def _add_chamada_curta(self, df):
        return df.withColumn(
            "chamada_curta",
            F.when(
                F.col("duracao_da_chamada") <= self.limiar_chamada_ofensora, 1
            ).otherwise(F.lit(0)),
        )

    def _add_chamada_autenticada(self, df):
        return df.withColumn(
            "chamada_autenticada",
            F.when(F.col("autenticacao").isNull(), self._AUTH_NONE)
            .when(F.col("autenticacao").contains("TN-Validation-Pa"), self._AUTH_PASS)
            .otherwise(F.lit(self._AUTH_FAIL)),
        )

    def _apply_standard_pipeline(
        self, df: DataFrame, date_time_fmt: str = "yyyy-MM-dd HH-mm-ss"
    ) -> DataFrame:
        df = self._format_columns(df, date_time_fmt)
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
        return df

    def _write_parquet(self, df: DataFrame, target_file: str) -> None:
        """
        Grava o DataFrame como parquet sem particionamento por coluna.

        Diferente da extração (particionada por tipo_de_chamada), a camada
        transformada é gravada flat porque o analyzer agrupa por
        numero_de_a_formatado + hora_da_chamada — o particionamento por
        tipo_de_chamada não traria pruning real nessa etapa.
        """
        df.withColumn(
            "tipo_de_chamada", F.col("tipo_de_chamada").cast(T.StringType())
        ).write.mode("overwrite").parquet(target_file)

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "TER"
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = (
            df.withColumn("chamada_autenticada", F.lit(0))
            .withColumn("chamada_caixa_postal", F.lit(0))
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_volte(self, source_file: str, target_file: str):
        """
        Transforma CDR no formato TIM VoLTE.

        Realiza duas leituras distintas sobre `source_file`:
        uma para identificar chamadas de caixa postal (tipo 'FORv')
        e outra para as chamadas principais (tipo 'TERv').

        Assume que `source_file` está particionado por `tipo_de_chamada`
        no estilo Hive (ex: tipo_de_chamada=FORv/). Sem esse particionamento,
        o arquivo será lido integralmente duas vezes, sem partition pruning.

        Args:
            source_file: Caminho para o diretório parquet particionado.

        Returns:
            DataFrame com as colunas definidas em _TRANSFORMED_COLUMNS.
        """
        df_voice_mail = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "FORv")
            .filter(
                (F.col("numero_de_b").startswith("5505"))
                & (F.length("numero_de_b") == 6)
            )
            .select("referencia")
            .distinct()
            .withColumn("chamada_caixa_postal", F.lit(1))
        )

        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "TERv"
        )

        df = self._apply_standard_pipeline(df)
        df = (
            df.join(df_voice_mail, on="referencia", how="left")
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .withColumn("chamada_autenticada", F.lit(0))
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_stir(self, source_file: str, target_file: str):
        sip_number_pattern = r"sip:(\d+)@"

        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "82"
        )

        df = self._format_columns(df)
        df = df.withColumn(
            "numero_de_b", F.regexp_extract(F.col("numero_de_b"), sip_number_pattern, 1)
        )
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
        df = self._add_chamada_autenticada(df)
        df = df.withColumn("chamada_caixa_postal", F.lit(0)).select(
            self._TRANSFORMED_COLUMNS
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_volte(self, source_file: str, target_file: str):
        """
        Transforma CDR no formato Vivo VoLTE.

        Realiza duas leituras distintas sobre `source_file`:
        uma para identificar chamadas de caixa postal (tipo '3')
        e outra para as chamadas principais (tipo '4').

        Assume que `source_file` está particionado por `tipo_de_chamada`
        no estilo Hive (ex: tipo_de_chamada=3/). Sem esse particionamento,
        o arquivo será lido integralmente duas vezes, sem partition pruning.

        Args:
            source_file: Caminho para o diretório parquet particionado.

        Returns:
            DataFrame com as colunas definidas em _TRANSFORMED_COLUMNS.
        """
        date_time_fmt = "yyyyMMdd HHmmss"
        voice_mail_columns_to_keep = [
            "referencia",
            "data_hora",
            "numero_de_b_formatado",
            "chamada_caixa_postal",
        ]
        df_voice_mail = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "3")
            .withColumn("numero_de_a_formatado", F.col("numero_de_a").substr(-11, 11))
            .withColumn("numero_de_b_formatado", F.col("numero_de_b").substr(-11, 11))
            .filter(F.col("numero_de_a_formatado") == F.col("numero_de_b_formatado"))
            .withColumn("chamada_caixa_postal", F.lit(1))
        )
        df_voice_mail = self._format_columns(df_voice_mail, date_time_fmt).select(
            voice_mail_columns_to_keep
        )

        df = (
            self.spark.read.parquet(source_file)
            .filter(F.col("tipo_de_chamada") == "4")
            .withColumn("autenticacao", F.split(F.col("numero_de_a"), ";").getItem(1))
            .withColumn("numero_de_a", F.split(F.col("numero_de_a"), ";").getItem(0))
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = self._add_chamada_autenticada(df)
        df = (
            df.join(
                df_voice_mail,
                on=["referencia", "data_hora", "numero_de_b_formatado"],
                how="left",
            )
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .select(self._TRANSFORMED_COLUMNS)
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
