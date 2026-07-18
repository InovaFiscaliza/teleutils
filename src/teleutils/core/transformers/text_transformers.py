from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.transformers.base_transformer import CDRBaseTransformer

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
    def transform_cdr_nokia(self, source_file: str, target_file: str):
        """Transforma CDR Nokia extraído para o schema final padronizado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Notes:
            O formato Nokia utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
            parsing de data/hora.
        """

        date_time_fmt = "yyyy-MM-dd HH:mm:ss"
        df = self.spark.read.parquet(source_file)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        # Corrige a coluna referencia hexadecimal BCD invertida no formato WORD:WORD:BYTE.
        # Exemplo: 'C407FEF101' -> '704C1FEF10'
        df = df.withColumn(
            "referencia",
            F.concat(
                # --- Primeiro WORD (C407 -> 704C) ---
                F.substring("_referencia", 4, 1),
                F.substring("_referencia", 3, 1),
                F.substring("_referencia", 2, 1),
                F.substring("_referencia", 1, 1),
                # --- Segundo WORD (FEF1 -> 1FEF) ---
                F.substring("_referencia", 8, 1),
                F.substring("_referencia", 7, 1),
                F.substring("_referencia", 6, 1),
                F.substring("_referencia", 5, 1),
                # --- BYTE final (01 -> 10) ---
                F.substring("_referencia", 10, 1),
                F.substring("_referencia", 9, 1),
            ),
        ).withColumn(
            "tipo_chamada",
            F.when(F.col("_tipo_chamada") == "FOR", "FORW").otherwise(
                F.col("_tipo_chamada")
            ),
        )

        # Adiciona colunas de informação de rota de entrada e saída, derivadas do campo _rota.
        # Regras para rota_saida
        # - Se "PTC" ou "FOR": recebe o valor direto de _rota
        # - Se "UCA": faz o split por "&&" e pega o último elemento (índice -1)
        # - Caso contrário: NULL
        outgoing_route_rules = (
            F.when(F.col("_tipo_chamada").isin("PTC", "FOR"), F.col("_rota"))
            .when(
                F.col("_tipo_chamada") == "UCA",
                F.element_at(F.split(F.col("_rota"), "&&"), -1),
            )
            .otherwise(F.lit(None))
        )

        # Regras para a rota_entrada
        # - Se "POC": recebe o valor direto de Routing_Category
        # - Caso contrário: NULL
        regra_rota_entrada = F.when(
            F.col("_tipo_chamada") == "POC", F.col("_rota")
        ).otherwise(F.lit(None))

        df = df.withColumn("rota_saida", outgoing_route_rules).withColumn(
            "rota_entrada", regra_rota_entrada
        )

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
        df = (
            df.withColumn(
                "tipo_chamada",
                F.when(F.col("_tipo_chamada") == "TERv", "tERMINATING-ROLE")
                .when(F.col("_tipo_chamada") == "ORIv", "oRIGINATING-ROLE")
                .when(F.col("_tipo_chamada") == "FORv", "cALLFORWARDING-ROLE")
                .otherwise(F.col("_tipo_chamada")),
            )
            .withColumn(
                "rota_entrada",
                F.lit(None),  # type: ignore
            )
            .withColumn(
                "rota_saida",
                F.lit(None),  # type: ignore
            )
        )  # Colunas ausentes no layout TIM Huawei

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
        df = self._preprocess_cdr_vivo_fcdr(df)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
