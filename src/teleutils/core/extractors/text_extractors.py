"""Módulo teleutils.core.extractors.text_extractors.

Responsável pela extração e padronização de registros de chamadas (CDR)
provenientes de múltiplos layouts de arquivo texto/CSV utilizados por
prestadoras e fornecedores distintos.

Principais responsabilidades:
    - Consumir contratos de mapeamento por formato via ``CDRTextSchema``.
    - Centralizar a leitura e validação de colunas em Spark.
    - Uniformizar nomes de colunas em uma estrutura comum para etapas seguintes.
    - Persistir o resultado em parquet para consumo das próximas etapas.

Principais funcionalidades:
    - Extração parametrizada por esquema (delimitador, índices, nomes, filtro).
    - Validação preventiva de configuração para reduzir falhas em runtime.
    - Inclusão de metadados de rastreabilidade do arquivo de origem.

Dependências relevantes:
    - pyspark.sql.SparkSession
    - pyspark.sql.functions
    - pyspark.sql.types
    - teleutils._logging.log_operation

Notes:
    Os índices de coluna em ``CDRTextSchema.column_indices`` são zero-based e devem
    corresponder exatamente ao layout do CSV após aplicação do delimitador.

Example:
    >>> extrator = CDRTextExtractor(spark)
    >>> df = extrator.extract_cdr_ericsson(
    ...     source_file="dados/ericsson.csv",
    ...     target_file="saida/ericsson"
    ... )
"""

from __future__ import annotations

import logging
from typing import ClassVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.extractors.schemas import CDRTextSchema, TEXT_DEFAULT_SCHEMAS

logger = logging.getLogger(__name__)


class CDRTextExtractor:
    """Orquestra a extração de CDR para um formato intermediário padronizado.

    Esta classe funciona como ponto de entrada para extração por tecnologia/
    fornecedor. Cada método público seleciona um esquema pré-definido e delega a
    execução para ``extract_cdr``, onde está o fluxo comum de processamento.

    O desenho separa lógica (implementação da extração) de configuração
    (mapeamentos em ``_SCHEMAS``), facilitando evolução e manutenção incremental.

    Attributes:
        spark: Sessão Spark utilizada para leitura e escrita de dados.

    Notes:
        Ponto de extensão principal: adição de novos formatos no dicionário
        ``_SCHEMAS`` e criação de um método público delegando para ``extract_cdr``.
    """

    # Schemas declarados como atributo de classe: são constantes e não dependem
    # de instância. Isso evita recriar os objetos a cada chamada e deixa a
    # configuração visível e fácil de manter no topo da classe.
    _SCHEMAS: ClassVar[dict[str, CDRTextSchema]] = TEXT_DEFAULT_SCHEMAS

    def __init__(self, spark: SparkSession) -> None:
        """Inicializa o extrator com uma sessão Spark ativa.

        Args:
            spark: Sessão Spark a ser reutilizada nas operações de extração.

        Notes:
            O construtor mantém apenas a sessão Spark necessária para executar
            leitura, seleção de colunas e escrita da saída intermediária.
        """
        self.spark = spark
        # SparkContext armazenado uma única vez, evitando chamadas repetidas
        # self._sc = spark.sparkContext

    def extract_cdr(
        self, source_file: str, target_file: str, schema: CDRTextSchema
    ) -> DataFrame:
        """Executa o pipeline de extração/normalização para um esquema CDR.

        Fluxo de processamento:
            1. Lê o CSV conforme delimitador/cabeçalho/schema informados.
            2. Valida existência dos índices solicitados no dataset lido.
            3. Seleciona e renomeia colunas para o contrato padronizado.
            4. Adiciona metadados de linhagem (prestadora, tipo_cdr, arquivo_origem).
            5. Aplica filtro opcional definido no schema.
            6. Persiste parquet de saída e relê o resultado.

        Args:
            source_file: Caminho do arquivo CSV de entrada.
            target_file: Diretório de saída em formato parquet.
            schema: Configuração de mapeamento aplicável ao formato de origem.

        Returns:
            DataFrame: Dados extraídos já persistidos e relidos do destino parquet.

        Raises:
            ValueError: Se algum índice requerido não existir no arquivo lido,
                cenário comum quando delimitador/header estão incorretos.
            FileNotFoundError: Se o caminho de entrada não existir.
            Exception: Erros propagados pelo Spark durante leitura/escrita.

        Notes:
            O retorno ocorre após releitura do parquet de saída, garantindo que o
            DataFrame refletirá exatamente o artefato persistido.

            Decisão arquitetural: a gravação é ``overwrite`` para simplificar
            reprocessamentos determinísticos do mesmo lote.
        """
        # self._sc.setJobDescription(schema.job_description)

        logger.info(
            "Lendo arquivo CSV: %s com delimitador '%s' e header=%s",
            source_file,
            schema.delimiter,
            schema.has_header,
        )
        df = self.spark.read.csv(
            source_file,
            sep=schema.delimiter,
            header=schema.has_header,
            schema=schema.schema,
            inferSchema=False,
        )

        # Valida se todos os índices solicitados existem no DataFrame lido.
        # Falhar cedo com mensagem clara é melhor do que erros crípticos do Spark.
        logger.info("Validando índices de coluna para o esquema '%s'", schema.name)
        max_index = max(schema.column_indices)
        if max_index >= len(df.columns):
            raise ValueError(
                f"Schema '{schema.name}' requer coluna no índice {max_index}, "
                f"mas o arquivo possui apenas {len(df.columns)} colunas.\n"
                f"Verifique se o delimitador '{schema.delimiter}' está correto "
                f"para o arquivo: {source_file}\n"
                f"Índices solicitados: {schema.column_indices}\n"
                f"Colunas disponíveis: {list(enumerate(df.columns))}\n"
                f"Configuração do schema: {schema!r}"
            )

        logger.info(
            "Selecionando e renomeando colunas conforme o esquema '%s'", schema.name
        )
        # A seleção por índice preserva compatibilidade com layouts sem cabeçalho
        # estável, onde nomes de coluna originais não são confiáveis.
        columns_to_keep = [f"`{df.columns[i]}`" for i in schema.column_indices]
        df = (
            df.select(columns_to_keep)
            .toDF(*schema.column_names)
            .withColumn(
                "prestadora", F.element_at(F.split(F.input_file_name(), "/"), -3)
            )
            .withColumn("tipo_cdr", F.element_at(F.split(F.input_file_name(), "/"), -2))
            .withColumn(
                "arquivo_origem", F.element_at(F.split(F.input_file_name(), "/"), -1)
            )
        )

        if schema.column_to_filter is not None:
            col_name, col_value = schema.column_to_filter
            logger.info(
                "Aplicando filtro: %s = '%s' para o esquema '%s'",
                col_name,
                col_value,
                schema.name,
            )
            # Regra de negócio configurável por schema: remove linhas de controle
            # específicas do fornecedor que não representam eventos válidos.
            df = df.filter(df[col_name] != col_value)

        logger.info(
            "Escrevendo DataFrame extraído para parquet: %s",
            target_file,
        )
        df.write.mode("overwrite").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai registros CDR no layout Ericsson.

        Args:
            source_file: Caminho do arquivo de entrada no formato Ericsson.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            DataFrame: Registros extraídos e normalizados do formato Ericsson.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.
            Exception: Erros propagados do pipeline Spark.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_ericsson(
            ...     source_file="dados/ericsson.csv",
            ...     target_file="parquet/ericsson_extracted"
            ... )
        """
        return self.extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

    @log_operation
    def extract_cdr_tim_huawei(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai registros CDR no layout TIM Huawei.

        Args:
            source_file: Caminho do arquivo de entrada no formato TIM Huawei.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            DataFrame: Registros extraídos e normalizados do formato TIM Huawei.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.
            Exception: Erros propagados do pipeline Spark.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_tim_huawei(
            ...     source_file="dados/tim_huawei.csv",
            ...     target_file="parquet/tim_huawei_extracted"
            ... )
        """
        return self.extract_cdr(source_file, target_file, self._SCHEMAS["tim_huawei"])

    @log_operation
    def extract_cdr_vivo_fcdr(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai registros CDR no layout Vivo FCDR.

        Args:
            source_file: Caminho do arquivo de entrada no formato Vivo FCDR.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            DataFrame: Registros extraídos e normalizados do formato Vivo FCDR.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.
            Exception: Erros propagados do pipeline Spark.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_vivo_fcdr(
            ...     source_file="dados/vivo_fcdr.csv",
            ...     target_file="parquet/vivo_fcdr_extracted"
            ... )
        """
        return self.extract_cdr(source_file, target_file, self._SCHEMAS["vivo_fcdr"])

    @log_operation
    def extract_cdr_nokia(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai registros CDR no layout Nokia.

        Args:
            source_file: Caminho do arquivo de entrada no formato Nokia.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            DataFrame: Registros extraídos e normalizados do formato Nokia.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.
            Exception: Erros propagados do pipeline Spark.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_nokia(
            ...     source_file="dados/nokia.csv",
            ...     target_file="parquet/nokia_extracted"
            ... )
        """
        df = self.extract_cdr(source_file, target_file, self._SCHEMAS["claro_nokia"])

        return df
