"""Módulo de extração e padronização inicial de CDRs do Teleparser.

Este módulo consolida a lógica de extração de CDRs em parquet para diferentes
fornecedores e layouts produzidos pelo Teleparser. O processo aplica mapeamento
de colunas para um schema intermediário comum, enriquece metadados de origem e
persiste o resultado para a etapa de transformação.

As definições de schema (dataclass ``CDRTeleparserSchema`` e os contratos
padrão por fornecedor) residem no módulo ``teleutils.core.extractors.schemas``,
mantendo aqui apenas a lógica de execução da extração.

Responsabilidades principais:
    - Ler arquivos parquet de entrada e projetar colunas padronizadas conforme
      um schema informado.
    - Adicionar metadados de proveniência (prestadora, tipo e arquivo).
    - Escrever parquet intermediário para consumo pelos transformadores.

Principais funcionalidades:
    - Extração para Ericsson.
    - Extração para TIM Huawei.
    - Extração para Vivo FCDR.
    - Extração para Nokia com tolerância a colunas ausentes.

Dependências relevantes:
    - pyspark.sql (DataFrame, SparkSession e funções colunares)
    - teleutils.core.extractors.schemas (contratos de mapeamento por fornecedor)
    - teleutils._logging.log_operation

Example:
    >>> extractor = CDRTeleparserExtractor(spark)
    >>> df = extractor.extract_cdr_ericsson("/tmp/origem", "/tmp/destino")
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.extractors.schemas import (
    TELEPARSER_DEFAULT_SCHEMAS,
    CDRTeleparserSchema,
)

logger = logging.getLogger(__name__)


class CDRTeleparserExtractor:
    """Executa extração de CDR parquet com mapeamento por fornecedor.

    A classe centraliza a leitura de dados do Teleparser, aplica projeção de
    colunas de acordo com um schema declarado e grava uma saída intermediária
    padronizada para o pipeline de transformação.

    Attributes:
        spark:
            Sessão Spark utilizada para leitura, projeção e escrita dos dados.
        schemas:
            Dicionário de contratos ``CDRTeleparserSchema`` disponíveis para
            extração, indexados por chave de fornecedor/layout.

    Notes:
        - Os schemas são injetados via construtor (``schemas``), permitindo
          substituir ou estender os contratos padrão sem alterar esta classe.
        - Quando nenhum schema é informado, ``TELEPARSER_DEFAULT_SCHEMAS`` (do
          módulo ``teleutils.core.extractors.schemas``) é utilizado.
        - Métodos públicos são wrappers sem lógica adicional significativa,
          mantendo o fluxo principal em ``extract_cdr``.
    """

    def __init__(
        self,
        spark: SparkSession,
        schemas: dict[str, CDRTeleparserSchema] | None = None,
    ) -> None:
        """Inicializa o extrator com sessão Spark ativa e schemas de mapeamento.

        Args:
            spark: Sessão Spark compartilhada pelo pipeline de extração.
            schemas: Dicionário de contratos ``CDRTeleparserSchema`` a utilizar.
                Quando omitido, os contratos padrão definidos em
                ``TELEPARSER_DEFAULT_SCHEMAS`` são adotados.

        Notes:
            - O uso de uma única sessão favorece consistência operacional e
              reaproveitamento de contexto em jobs encadeados.
            - A injeção de dependência de ``schemas`` permite testar a classe
              com contratos customizados e adicionar novos fornecedores sem
              modificar esta classe.
        """
        self.spark = spark
        self.schemas = schemas if schemas is not None else TELEPARSER_DEFAULT_SCHEMAS
        # self._sc = spark.sparkContext

    def extract_cdr(
        self,
        source_file: str,
        target_file: str,
        schema: CDRTeleparserSchema,
        ignore_missing_columns: bool = False,
        unique: bool = False,
    ) -> DataFrame:
        """Executa extração genérica conforme schema de mapeamento informado.

        Fluxo de processamento:
            1. Lê parquet de entrada.
            2. Valida presença de colunas requeridas (ou ignora, conforme flag).
            3. Aplica seleção e renomeação com base no schema.
            4. Enriquece metadados de origem a partir do caminho do arquivo.
            5. Remove duplicatas opcionalmente.
            6. Persiste parquet intermediário e relê resultado final.

        Args:
            source_file: Caminho do parquet de entrada.
            target_file: Caminho do parquet de saída intermediária.
            schema: Contrato de mapeamento a ser aplicado.
            ignore_missing_columns: Define se colunas ausentes devem ser
                toleradas (selecionando apenas as presentes).
            unique: Define se duplicatas devem ser removidas no resultado.

        Returns:
            DataFrame: DataFrame relido de ``target_file`` após escrita.

        Raises:
            ValueError:
                Quando colunas obrigatórias do schema estão ausentes e
                ``ignore_missing_columns`` é ``False``.
            AnalysisException:
                Propagada pelo Spark em erros de leitura/escrita parquet.

        Notes:
            - Regra de negócio: metadados ``prestadora``, ``tipo_cdr`` e
              ``arquivo_origem`` são derivados da hierarquia do path de entrada.
            - Efeito colateral: grava dados em ``target_file`` com overwrite.
            - Anotação de manutenção: qualquer mudança no padrão de diretórios
              de origem impacta a extração de metadados via ``input_file_name``.
        """
        # self._sc.setJobDescription(schema.job_description)

        logger.info("Lendo arquivo parquet: %s", source_file)
        if isinstance(source_file, list):
            df = self.spark.read.parquet(*source_file)
        else:
            df = self.spark.read.parquet(source_file)

        logger.info(
            "Parâmetro ignore_missing_columns: %s. %s colunas ausentes.",
            ignore_missing_columns,
            "Ignorando" if ignore_missing_columns else "Verificando",
        )

        missing_columns = [
            source_col
            for source_col, _ in schema.column_mapping
            if source_col not in df.columns
        ]

        if missing_columns and not ignore_missing_columns:
            raise ValueError(
                f"Schema '{schema.name}' requer colunas ausentes no parquet: "
                f"{missing_columns}. Colunas disponiveis: {df.columns}"
            )

        if missing_columns and ignore_missing_columns:
            logger.warning(
                "Colunas ausentes no parquet: %s. Selecionando apenas colunas presentes.",
                missing_columns,
            )

        # Filtrar o mapeamento para incluir apenas colunas presentes quando ignore_missing_columns=True
        filtered_column_mapping = (
            [
                (source_col, target_col)
                for source_col, target_col in schema.column_mapping
                if source_col in df.columns
            ]
            if ignore_missing_columns
            else schema.column_mapping
        )

        # Colunas com ponto representam caminhos de campo (nested) e exigem
        # escaping com crases para evitar interpretação incorreta pelo Spark SQL.
        select_expr = [
            F.col(f"`{source_col}`").alias(target_col)
            if "." in source_col
            else F.col(source_col).alias(target_col)
            for source_col, target_col in filtered_column_mapping
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

        if unique:
            df = df.dropDuplicates()
            logger.info(
                "Parâmetro unique: %s. Removendo duplicatas.",
                unique,
            )

        if "_tipo_cdr" in df.columns:
            df = df.withColumn("tipo_cdr", F.col("_tipo_cdr")).drop("_tipo_cdr")

        logger.info("Escrevendo DataFrame extraido para parquet: %s", target_file)
        df.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Ericsson para parquet intermediário.

        Args:
            source_file: Caminho do parquet de entrada Ericsson.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            Delega integralmente para ``extract_cdr`` com schema Ericsson.
        """
        return self.extract_cdr(source_file, target_file, self.schemas["ericsson"])

    @log_operation
    def extract_cdr_tim_huawei(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR TIM Huawei com remoção de duplicatas.

        Args:
            source_file: Caminho do parquet de entrada TIM Huawei.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            - Regra de negócio: ``unique=True`` para reduzir duplicidade de
              registros observada neste layout.
            - Ponto de manutenção: validar periodicamente o impacto dessa
              deduplicação em cenários de retentativa de ingestão.
        """
        df = self.extract_cdr(
            source_file, target_file, self.schemas["tim_huawei"], unique=True
        )
        return df

    @log_operation
    def extract_cdr_vivo_huawei(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Vivo Huawei para parquet intermediário.

        Args:
            source_file: Caminho do parquet de entrada Vivo Huawei.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            Delega para ``extract_cdr`` com schema Vivo Huawei sem ajustes
            adicionais de tolerância ou deduplicação.
        """
        df = self.extract_cdr(source_file, target_file, self.schemas["vivo_huawei"])
        return df

    @log_operation
    def extract_cdr_nokia(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR Nokia com tolerância a colunas ausentes.

        Args:
            source_file: Caminho do parquet de entrada Nokia.
            target_file: Caminho do parquet intermediário de saída.

        Returns:
            DataFrame: Resultado da extração relido de ``target_file``.

        Notes:
            - Regra de negócio: ``ignore_missing_columns=True`` para acomodar
              variações de disponibilidade entre tipos de CDR Nokia.
            - Anotação de manutenção: sempre revisar logs de colunas ausentes
              para identificar mudanças de layout na origem.
        """
        df = self.extract_cdr(
            source_file,
            target_file,
            self.schemas["nokia"],
            ignore_missing_columns=True,
        )
        return df
