from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.transformers._base_transformer import CDRBaseTransformer


class CDRTeleparserTransformer(CDRBaseTransformer):
    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador com sessão Spark ativa.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.
        """

        self.spark = spark

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        date_time_fmt = "yy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        col = F.col("duracao")
        hours = F.substring(col, 1, 2).cast("int") * 3600
        minutes = F.substring(col, 4, 2).cast("int") * 60
        seconds = F.substring(col, 7, 2).cast("int")
        df = df.withColumn(
            "duracao",
            F.when(col.isNotNull(), hours + minutes + seconds).otherwise(0).cast("int"),
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_ats(self, source_file: str, target_file: str):
        date_time_fmt = "yyyy-MM-dd HH:mm:ssxxx"
        df = self.spark.read.parquet(source_file)

        df = df.withColumn(
            "numero_origem",
            F.regexp_extract("_numero_origem", r"\+([^@;]+)", 1),
        ).withColumn(
            "_autenticacao",
            F.when(
                F.col("_numero_origem").contains(";"),
                F.regexp_extract("_numero_origem", r";\s*(.+)$", 1),
            ).otherwise(F.lit(None)),
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
