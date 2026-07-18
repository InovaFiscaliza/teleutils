from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
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

        df = (
            # Excluir registros com referência nula, pois não são válidos para análise.
            df.filter(F.col("referencia").isNotNull())
            # Extrair autenticação e prefixos adicionais dos números.
            # A autenticação está contida na coluna _numero_origem_generico,
            # por exemplo: verstat=TN-Validation-Passed
            .withColumn(
                "_autenticacao",
                F.regexp_extract(
                    "_numero_origem_generico", r"(verstat=[a-zA-Z\-]+)", 0
                ),
            )
            # Remover caracteres adicionais dos números de telefone, mantendo apenas os 20  caracteres.
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
            .withColumn(
                "numero_origem", F.col("numero_origem").substr(3, 9999)
            )  # spark exige o terceiro argumento para substr, mesmo que seja maior que o tamanho da string
            .withColumn("numero_destino", F.col("numero_destino").substr(3, 9999))
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

        # CDRs Nokia possuem um campo de duração específico para cada tipo, apenas um com valor não nulo por registro.
        # A expressão a seguir garante apenas uma coluna com duracao final preenchida com o valor correto, independentemente do tipo de CDR.
        duration_columns = [col for col in df.columns if col.startswith("_duracao")]
        coalesce_duration_expression = [F.col(c) for c in duration_columns]
        coalesce_duration_expression.append(F.lit("0"))
        coalesce_duration_expression = F.coalesce(*coalesce_duration_expression)
        df = df.withColumn("duracao", coalesce_duration_expression)

        # Alguns CDRs não contém valor em data_hora_alocacao_canal, mas possuem data_hora_referencia preenchida.
        # A expressão a seguir garante que a coluna data_hora final seja preenchida, ainda que por nulo, independentemente do tipo de CDR.
        df = df.withColumn(
            "data_hora",
            F.coalesce(
                F.col("data_hora_alocacao_canal"), F.col("data_hora_referencia")
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
        # O script legado utiliza as colunas `orig_calling_number` e `forwarding_number` para derivar os campos numero_origem e numero_destino.
        # Os campos utilizados por esse script (numero_origem_original e numero_destino_original) mostraram o mesmo resultados e estão mais aderentes à documentação Nokia, com erro muito pequeno em relação ao parser legado.
        # Validar se quando a chamada é encaminhada para a caixa postal ocorre que o número de destino encaminhar para si mesmo a chamada.
        # Em exemplo analisado onde cause_for_forwarding = `SCP initiated`, forwarding_number = `8885561993363275` e forwared_to_number = `C145561993363275`, o que pode indicar que a chamada foi encaminhada para a caixa postal do próprio número de destino.
        df = df.withColumn(
            "numero_origem",
            F.coalesce(F.col("numero_origem"), F.col("numero_origem_original")),
        ).withColumn(
            "numero_destino",
            F.when(
                F.col("tipo_chamada") == "FORW", F.col("numero_origem_encaminhamento")
            ).otherwise(
                F.col("numero_destino"),
            ),
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
