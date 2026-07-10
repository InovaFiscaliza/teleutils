from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from teleutils._logging import log_operation
from teleutils.core.transformers._nokia import NOKIA_RECORD_TYPE_MAPPING
from teleutils.core.transformers.base_transformer import CDRBaseTransformer


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

    @log_operation
    def transform_cdr_nokia(self, source_file: str, target_file: str):
        date_time_fmt = "dd/MM/yyyy HH:mm:ss"
        df = self.spark.read.parquet(source_file)

        # Teleparser Nokia traz a descrição do campo record_type, o mapeamento a seguir converte para sigla.
        record_type_mapping_expr = F.create_map(
            *[F.lit(x) for x in chain(*NOKIA_RECORD_TYPE_MAPPING.items())]
        )
        df = df.withColumn(
            "tipo_chamada", record_type_mapping_expr[F.col("_tipo_chamada")]
        )

        # CDRs Nokia possuem um campo de duração específico para cada tipo, apenas um com valor não nulo por registro.
        # A expressão a seguir garante apenas uma coluna com duracao final preenchida com o valor correto, independentemente do tipo de CDR.
        duration_columns = [col for col in df.columns if col.startswith("_duracao")]
        coalesce_duration_expression = [F.col(c) for c in duration_columns]
        coalesce_duration_expression.append(F.lit("0"))
        coalesce_duration_expression = F.coalesce(*coalesce_duration_expression)
        df = df.withColumn("duracao", coalesce_duration_expression)

        # Alguns CDRs não contém valor em data_hora_alocacao_canal, mas possuem data_hora_referencia preenchida.
        # A expressão a seguir garante que a coluna data_hora final seja preenchida, ainda que por nulo, independentemente do tipo de CDR.
        coalesce_date_time_expression = F.coalesce(
            F.col("data_hora_alocacao_canal"), F.col("data_hora_referencia")
        )
        df = df.withColumn(
            "data_hora",
            F.when(coalesce_date_time_expression.startswith("00"), None).otherwise(
                coalesce_date_time_expression
            ),
        )

        # CDRs do tipo FORW não possuem os campos calling_number e called_number
        # considerar os campos alternativos mapeados no extrator:
        #
        # +-------------------------+-------------------------------+
        # | Antes (CDR Bruto)       | Depois (CDR Extraído)         |
        # |-------------------------|-------------------------------|
        # | calling_number          | _numero_origem                |
        # | orig_calling_number     | numero_origem_original        |
        # | called_number           | _numero_destino               |
        # | orig_called_number      | numero_destino_original       |
        # | forwarding_number       | numero_origem_encaminhamento  |
        # | forwarded_to_number     | numero_destino_encaminhamento |
        # +------------------------+-------------------------------+
        #
        # O script legado utiliza as colunas numero_origem_original e numero_origem_encaminhamento para derivar os campos numero_origem e numero_destino.
        # Os campos utilizados por esse script (numero_origem_original e numero_destino_original) mostraram o mesmo resultados e estão mais aderentes à documentação Nokia, portanto foram mantidos.
        # A execução com F.coalesce() garante que o mapeamento seja feito de maneira mais performática do que com F.when().otherwise().
        df = df.withColumn(
            "numero_origem",
            F.coalesce(F.col("numero_origem"), F.col("numero_origem_original")),
        ).withColumn(
            "numero_destino",
            F.when(
                F.col("tipo_chamada") == "FORW", F.col("numero_destino_original")
            ).otherwise(
                F.col("numero_destino"),
            ),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
