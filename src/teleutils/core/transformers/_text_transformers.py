from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.transformers._base_transformer import CDRBaseTransformer

logger = logging.getLogger(__name__)


class CDRTextTransformer(CDRBaseTransformer):
    """Aplica regras de transformação para CDRs extraídos em parquet.

    Classe responsável por converter datasets intermediários de extração para um
    schema de consumo padronizado no domínio. Os métodos públicos representam
    entradas por tipo de layout/origem, enquanto métodos privados concentram
    etapas reutilizáveis de transformação.

    Attributes:
        spark: Sessão Spark utilizada para leitura, transformação e escrita.

    Notes:
        - O pipeline padrão é centralizado em ``_apply_standard_pipeline`` para
          evitar divergência entre formatos.
        - Ponto de extensão: novos formatos devem reutilizar o pipeline comum e
          ajustar somente pré-processamentos específicos quando necessários.
    """

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
        """Transforma CDR Ericsson extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            O formato Ericsson utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
            parsing de data/hora.
        """

        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df, date_time_fmt)
        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "TER", "msTerminating")
            .when(F.col("_tipo_chamada") == "TRA", "transit")
            .when(F.col("_tipo_chamada") == "ORI", "msOriginating")
            .when(F.col("_tipo_chamada") == "ROA", "roamingCallForwarding")
            .when(F.col("_tipo_chamada") == "FOR", "callForwarding")
            .otherwise(F.col("_tipo_chamada")),
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_claro_nokia(self, source_file: str, target_file: str):
        """Transforma CDR Claro Nokia extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            O formato Claro Nokia utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
            parsing de data/hora.
        """

        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_tim_huawei(self, source_file: str, target_file: str):
        """Transforma CDR TIM Huawei extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            Utiliza formato temporal padrão do pipeline
            (``yyyy-MM-dd HH-mm-ss``), salvo ajuste explícito.
        """

        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df)
        df = df.withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "TERv", "tERMINATING-ROLE")
            .when(F.col("_tipo_chamada") == "ORIv", "oRIGINATING-ROLE")
            .when(F.col("_tipo_chamada") == "FORv", "cALLFORWARDING-ROLE")
            .otherwise(F.col("_tipo_chamada")),
        )

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_fcdr(self, source_file: str, target_file: str):
        """Transforma CDR Vivo FCDR extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            - Regra de negócio específica: ``numero_origem`` pode vir composto
              com metadado de autenticação separado por ``;``.
            - A transformação extrai o número para ``numero_origem`` e move a
              segunda parte para ``_autenticacao``.
            - O formato temporal esperado neste layout é ``yyyyMMdd HHmmss``.
        """

        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)

        # Extrair autenticação e prefixos adicionais dos números.
        # A autenticação está contida na coluna _numero_origem,
        # por exemplo: 551136128860;verstat=TN-Validation-Passe
        df = (
            df.withColumn("_split", F.split(F.col("_numero_origem"), ";"))
            .withColumn("numero_origem", F.col("_split").getItem(0))
            .withColumn("_autenticacao", F.col("_split").getItem(1))
            .drop("_split")
        )

        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
