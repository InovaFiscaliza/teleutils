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

        super().__init__(spark)

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
    def transform_cdr_tim_huawei(self, source_file: str, target_file: str):
        date_time_fmt = "yyyy-MM-dd HH:mm:ssxxx"
        df = self.spark.read.parquet(source_file)

        # Extrair autenticação e prefixos adicionais dos números.
        # A autenticação está contida na coluna _numero_origem_generico,
        # por exemplo: verstat=TN-Validation-Passed

        # As colunas numero_origem e numero_destino contêm os números dos terminais
        # precedidos de prefixos adicionais (11 ou 14) que devem ser removidos:
        #
        # +-----------------|---------------+
        # | Antes           | Depois        |
        # |-----------------|---------------|
        # | 1440042704      | 40042704      |
        # | 115595981241366 | 5595981241366 |
        # | 1408000910091   | 08000910091   |
        # +-----------------|---------------+
        #
        # O resultado final foi limitado a 15 caracteres, tamanho máximo de um número
        # de telefone estabelecido no padrão internacional ITU-T E.164

        df = (
            df.withColumn(
                "_autenticacao",
                F.regexp_extract(
                    "_numero_origem_generico", r"(verstat=[a-zA-Z\-]+)", 0
                ),
            )
            .withColumn("numero_origem", F.col("numero_origem").substr(3, 15))
            .withColumn("numero_destino", F.col("numero_destino").substr(3, 15))
        )
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_fcdr(self, source_file: str, target_file: str):

        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)
        df = self._preprocess_cdr_vivo_fcdr(df)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
