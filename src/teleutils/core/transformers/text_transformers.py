"""Módulo de transformação de CDRs oriundos de extratores textuais.

Este módulo concentra a transformação de CDRs já extraídos para parquet,
normalizando diferenças de layout entre fornecedores e preparando o contrato
final de dados utilizado no domínio analítico.

Responsabilidades principais:
    - Ler datasets intermediários de CDR por fornecedor.
    - Aplicar pré-processamentos específicos de campos e códigos.
    - Delegar normalizações comuns para o pipeline base compartilhado.
    - Persistir o resultado no schema padronizado da aplicação.

Principais funcionalidades:
    - Transformação de CDR Ericsson.
    - Transformação de CDR Nokia.
    - Transformação de CDR TIM Huawei.
    - Transformação de CDR Vivo FCDR.

Dependências relevantes:
    - pyspark.sql (SparkSession e funções colunares)
    - teleutils._logging.log_operation
    - teleutils.core.transformers.base_transformer.CDRBaseTransformer

Notes:
    - Estado atual do projeto: o desenvolvimento deste módulo está congelado.
    - A função ``_select_transformed_columns`` foi fixada como contrato estável
      para permitir a evolução dos demais módulos sem risco de divergência de
      schema de saída.
    - Mudanças futuras neste arquivo devem priorizar correções críticas e
      preservar o contrato já consolidado.

Example:
    >>> transformer = CDRTextTransformer(spark)
    >>> df = transformer.transform_cdr_ericsson("/tmp/origem", "/tmp/destino")
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
        - Contexto de manutenção: este módulo está congelado na fase atual do
          projeto e atua como referência estável para o contrato de saída.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """Inicializa o transformador textual com sessão Spark ativa.

        Objetivo da operação:
            Registrar a sessão Spark compartilhada para execução de leituras,
            transformações distribuídas e persistência no pipeline.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de transformação.

        Notes:
            A sessão é encaminhada para a classe base, onde ficam utilitários
            comuns de normalização e escrita final.
        """

        super().__init__(spark)

    def _select_transformed_columns(self, df: DataFrame) -> DataFrame:
        """Seleciona e renomeia colunas para o contrato final do domínio.

        Este método foi intencionalmente fixado no estado atual do projeto para
        estabilizar o schema de saída consumido por módulos em desenvolvimento.

        Args:
            df: DataFrame após aplicação do pipeline padrão.

        Returns:
            DataFrame: DataFrame no schema padronizado de saída.

        Notes:
            - Regra de negócio: ``tipo_chamada`` é forçado para string para
              uniformizar integração entre diferentes origens.
            - Anotação de manutenção: qualquer alteração de contrato de saída
              deve ocorrer neste método para preservar consistência.
            - Estado de congelamento: evitar mudanças de mapeamento/alias sem
              coordenação explícita com os módulos downstream dependentes.
        """
        return df.withColumn(
            "tipo_chamada", F.col("tipo_chamada").cast(T.StringType())
        ).select(
            F.col("referencia").alias("nu_referencia"),
            F.col("numero_origem").alias("nu_origem_original"),
            F.col("numero_destino").alias("nu_destino_original"),
            F.col("numero_origem_formatado").alias("nu_origem"),
            F.col("numero_origem_valido").alias("ic_origem_valido"),
            F.col("numero_destino_formatado").alias("nu_destino"),
            F.col("numero_destino_valido").alias("ic_destino_valido"),
            F.col("data_hora").alias("dh_chamada"),
            F.col("duracao").alias("qt_duracao_segundos"),
            F.col("tipo_chamada").alias("no_tipo_chamada"),
            F.col("autenticacao").alias("no_autenticacao"),
            F.col("rota_entrada").alias("no_rota_entrada"),
            F.col("rota_saida").alias("no_rota_saida"),
            F.col("prestadora").alias("no_prestadora"),
            F.col("tipo_cdr").alias("no_tipo_cdr"),
            F.col("arquivo_origem").alias("no_arquivo_origem"),
        )

    @log_operation
    def transform_cdr_ericsson(self, source_file: str, target_file: str):
        """Transforma CDR Ericsson extraído para o schema final padronizado.

        Objetivo da operação:
            Normalizar CDRs Ericsson, convertendo códigos de tipo de chamada
            para rótulos de domínio e aplicando pipeline comum de qualidade.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Raises:
            AnalysisException:
                Propagada pelo Spark caso o arquivo de entrada não exista ou
                apresente schema incompatível com as transformações esperadas.

        Notes:
            - O formato Ericsson utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
              parsing de data/hora.
            - Efeito colateral: grava parquet em ``target_file`` com modo de
              sobrescrita (definido no transformador base).
            - Anotação de manutenção: novos códigos em ``_tipo_chamada`` devem
              ser mapeados nesta cadeia de ``when`` para manter semântica.
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

        Objetivo da operação:
            Corrigir particularidades de encoding do layout Nokia (referência
            BCD invertida, regras de rota e ajuste de tipo de chamada) antes da
            persistência no contrato final.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Raises:
            AnalysisException:
                Propagada pelo Spark em falhas de leitura ou ausência de colunas
                necessárias para as regras específicas deste layout.

        Notes:
            - O formato Nokia utiliza máscara ``yyyy-MM-dd HH:mm:ss`` para
              parsing de data/hora.
            - Efeito colateral: grava parquet em ``target_file``.
            - Anotação de manutenção: o algoritmo de reordenação de
              ``_referencia`` depende do padrão WORD:WORD:BYTE e deve ser
              revisado se a origem alterar o layout binário.
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

        Objetivo da operação:
            Aplicar pipeline comum e mapear variantes de ``_tipo_chamada`` para
            terminologia de domínio, preenchendo campos de rota ausentes com
            nulo tipado.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Raises:
            AnalysisException:
                Propagada pelo Spark em problemas de leitura ou incompatibilidade
                de schema da entrada.

        Notes:
            - Utiliza formato temporal padrão do pipeline
              (``yyyy-MM-dd HH-mm-ss``), salvo ajuste explícito.
            - Efeito colateral: grava parquet em ``target_file``.
            - Regra de negócio: ``rota_entrada`` e ``rota_saida`` são nulas por
              inexistirem no layout TIM Huawei deste extrator.
            - Anotação de manutenção: revisar mapeamentos se novas siglas
              ``*_v`` forem introduzidas na origem.
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
                F.lit(None).cast("string"),  # type: ignore
            )
            .withColumn(
                "rota_saida",
                F.lit(None).cast("string"),  # type: ignore
            )
        )  # Colunas ausentes no layout TIM Huawei

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def transform_cdr_vivo_fcdr(self, source_file: str, target_file: str):
        """Transforma CDR Vivo FCDR extraído para o schema final padronizado.

        Objetivo da operação:
            Decompor campos compostos do layout Vivo/FCDR e aplicar o pipeline
            comum com máscara temporal específica da origem.

        Args:
            source_file: Caminho do parquet intermediário de entrada.
            target_file: Caminho do parquet final de saída.

        Returns:
            DataFrame: DataFrame relido do parquet transformado em ``target_file``.

        Raises:
            AnalysisException:
                Propagada pelo Spark em falhas de leitura ou schema inesperado
                para o pré-processamento específico de Vivo/FCDR.

        Notes:
            - Regra de negócio específica: ``numero_origem`` pode vir composto
              com metadado de autenticação separado por ``;``.
            - A transformação extrai o número para ``numero_origem`` e move a
              segunda parte para ``_autenticacao``.
            - O formato temporal esperado neste layout é ``yyyyMMdd HHmmss``.
            - Efeito colateral: grava parquet em ``target_file``.
            - Integração relevante: reutiliza ``_preprocess_cdr_vivo_fcdr`` da
              classe base para manter regra centralizada.
        """

        date_time_fmt = "yyyyMMdd HHmmss"
        df = self.spark.read.parquet(source_file)
        df = self._preprocess_cdr_vivo_fcdr(df)
        df = self._apply_standard_pipeline(df, date_time_fmt)

        self._write_parquet(df, target_file)
        return self.spark.read.parquet(target_file)
