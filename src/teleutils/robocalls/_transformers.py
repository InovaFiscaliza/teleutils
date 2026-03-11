import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

from teleutils.preprocessing import normalize_number

TRANSFORMED_COLUMNS = [
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
LIMIAR_CHAMADA_OFENSORA = 6


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
    def __init__(
        self,
        spark: SparkSession,
        limiar_chamada_ofensora: int = LIMIAR_CHAMADA_OFENSORA,
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
            F.when(F.col("duracao_da_chamada") <= LIMIAR_CHAMADA_OFENSORA, 1).otherwise(
                F.lit(0)
            ),
        )

    def transform_cdr_ericsson(self, source_file: str):
        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "TER"
        )
        df = self._format_columns(df, "yyyy-MM-dd HH:mm:ss")
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
        df = (
            df.withColumn("chamada_autenticada", F.lit(0))
            .withColumn("chamada_caixa_postal", F.lit(0))
            .select(TRANSFORMED_COLUMNS)
        )

        return df

    def transform_cdr_tim_volte(self, source_file: str):
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

        df = self._format_columns(df)
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
        df = (
            df.join(df_voice_mail, on="referencia", how="left")
            .withColumn(
                "chamada_caixa_postal",
                F.coalesce(F.col("chamada_caixa_postal"), F.lit(0)),
            )
            .withColumn("chamada_autenticada", F.lit(0))
            .select(TRANSFORMED_COLUMNS)
        )

        return df

    def transform_cdr_tim_stir(self, source_file: str):
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
        df = (
            df.withColumn(
                "chamada_autenticada",
                F.when(
                    F.col("autenticacao").startswith("TN-Validation-Pa"), 1
                ).otherwise(F.lit(0)),
            )
            .withColumn("chamada_caixa_postal", F.lit(0))
            .select(TRANSFORMED_COLUMNS)
        )

        return df

    def transform_cdr_vivo_volte(self, source_file: str):
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

        df = self.spark.read.parquet(source_file).filter(
            F.col("tipo_de_chamada") == "4"
        )
        df = self._format_columns(df, date_time_fmt)
        df = self._format_numbers(df)
        df = self._add_chamada_curta(df)
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
            .withColumn("chamada_autenticada", F.lit(0))
            .select(TRANSFORMED_COLUMNS)
        )

        return df
