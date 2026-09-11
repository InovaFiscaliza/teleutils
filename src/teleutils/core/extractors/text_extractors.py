"""Extração e padronização de CDRs provenientes de arquivos texto/CSV.

Este módulo implementa o fluxo comum para ler arquivos delimitados com Spark,
selecionar colunas por posição, renomeá-las conforme um contrato
``CDRTextSchema`` e persistir o resultado em Parquet. O catálogo de layouts é
definido em ``teleutils.core.extractors.schemas.text``; a classe deste módulo
coordena a execução e mantém os métodos de entrada específicos de cada layout.

Principais responsabilidades:
    - Consumir contratos de mapeamento por formato via ``CDRTextSchema``.
    - Ler e validar a quantidade de colunas disponibilizada pelo arquivo.
    - Uniformizar nomes de colunas e adicionar metadados de origem.
    - Aplicar filtros de registros definidos pelo contrato de entrada.
    - Persistir o DataFrame intermediário em Parquet e retornar seu caminho.

Principais funcionalidades:
    - Extração parametrizada por delimitador, cabeçalho, schema Spark, índices,
      nomes de saída e filtro opcional.
    - Validação preventiva do maior índice solicitado antes da seleção.
    - Inclusão das colunas ``prestadora``, ``tipo_cdr`` e ``arquivo_origem`` a
      partir do caminho retornado por ``input_file_name``.

Dependências relevantes:
    - pyspark.sql.SparkSession
    - pyspark.sql.functions
    - teleutils._logging.log_operation
    - teleutils.core.extractors.schemas.text.CDRTextSchema

Notes:
    Os índices de coluna em ``CDRTextSchema.column_indices`` são zero-based e
    devem corresponder às colunas produzidas pelo Spark após a aplicação do
    delimitador e do cabeçalho configurados.

    A escrita do resultado usa modo ``overwrite``. O diretório de destino é
    portanto substituído a cada execução do método ``extract_cdr``.

Example:
    >>> extrator = CDRTextExtractor(spark)
    >>> destino = extrator.extract_cdr_algar_huawei(
    ...     source_file="dados/algar_ngn.csv",
    ...     target_file="saida/algar_ngn"
    ... )
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils._logging import log_operation
from teleutils.core.extractors.schemas import CDRTextSchema, TEXT_DEFAULT_SCHEMAS

logger = logging.getLogger(__name__)


class CDRTextExtractor:
    """Orquestra a extração de CDR texto/CSV para um formato intermediário.

    Cada método público de layout seleciona um contrato de
    ``TEXT_DEFAULT_SCHEMAS`` e delega a execução para ``extract_cdr``, onde está
    o fluxo comum de leitura, seleção, enriquecimento, filtragem e persistência.

    A separação entre a execução e os mapeamentos em ``schemas`` permite alterar
    configurações de layout sem duplicar o processamento Spark.

    Attributes:
        spark: Sessão Spark utilizada para leitura e escrita de dados.
        schemas: Dicionário de contratos de mapeamento indexados por chave de
            fornecedor ou layout.

    Notes:
        Quando nenhum dicionário é fornecido ao construtor, ``schemas`` referencia
        o catálogo compartilhado ``TEXT_DEFAULT_SCHEMAS``. Para adicionar um novo
        ponto de entrada, é necessário incluir o contrato correspondente no
        catálogo e um método que o encaminhe a ``extract_cdr``.

        O método ``extract_cdr`` recebe explicitamente o schema que será usado;
        os métodos específicos obtêm esse schema no atributo ``schemas``.
    """

    def __init__(
        self, spark: SparkSession, schemas: dict[str, CDRTextSchema] | None = None
    ) -> None:
        """Inicializa o extrator com uma sessão Spark ativa.

        Args:
            spark: Sessão Spark a ser reutilizada nas operações de extração.
            schemas: Dicionário opcional de contratos por layout. Quando omitido,
                a instância usa ``TEXT_DEFAULT_SCHEMAS``.

        Notes:
            A sessão Spark é mantida em ``self.spark`` e o valor de ``schemas`` é
            mantido em ``self.schemas``. Nenhum arquivo é lido ou escrito durante
            a inicialização.
        """
        self.spark = spark
        self.schemas = schemas if schemas is not None else TEXT_DEFAULT_SCHEMAS
        # SparkContext armazenado uma única vez, evitando chamadas repetidas
        # self._sc = spark.sparkContext

    def extract_cdr(
        self, source_file: str, target_file: str, schema: CDRTextSchema
    ) -> str:
        """Lê, seleciona, renomeia, filtra e persiste registros de um layout CDR.

        Fluxo de processamento:
            1. Lê o arquivo delimitado com as opções do ``schema``.
            2. Verifica se o maior índice solicitado existe no DataFrame lido.
            3. Seleciona as colunas por posição e aplica ``column_names``.
            4. Adiciona ``prestadora``, ``tipo_cdr`` e ``arquivo_origem`` a partir
               do caminho do arquivo de entrada.
            5. Mantém registros diferentes do valor do filtro opcional do schema.
            6. Sobrescreve o destino em Parquet.

        Args:
            source_file: Caminho do arquivo CSV de entrada.
            target_file: Diretório de saída em formato parquet.
            schema: Configuração de mapeamento aplicável ao formato de origem.
                Define as opções de leitura, as posições selecionadas, os nomes
                de saída e o filtro opcional.

        Returns:
            str: Caminho do diretório Parquet persistido em ``target_file``.

        Raises:
            ValueError: Se algum índice requerido não existir no arquivo lido,
                geralmente indicando incompatibilidade entre o layout do arquivo
                e as opções de delimitador ou cabeçalho.

        Notes:
            A seleção de colunas usa posições, e não nomes de origem, porque os
            contratos também suportam arquivos sem cabeçalho confiável.

            O filtro é aplicado depois da seleção e renomeação; por isso, seu
            primeiro elemento deve corresponder a um nome presente em
            ``schema.column_names``. A condição de desigualdade do Spark não
            mantém valores nulos na coluna filtrada.
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
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True,
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
        columns_to_keep = [
            F.col(df.columns[index]).alias(column_name)
            for index, column_name in zip(
                schema.column_indices,
                schema.column_names,
            )
        ]
        df = df.select(
            *columns_to_keep,
            F.element_at(F.split(F.input_file_name(), "/"), -3).alias("prestadora"),
            F.element_at(F.split(F.input_file_name(), "/"), -2).alias("tipo_cdr"),
            F.url_decode(F.element_at(F.split(F.input_file_name(), "/"), -1)).alias(
                "arquivo_origem"
            ),
        )

        if schema.column_to_filter is not None:
            col_name, col_value = schema.column_to_filter
            logger.info(
                "Aplicando filtro: %s = '%s' para o esquema '%s'",
                col_name,
                col_value,
                schema.name,
            )
            df = df.filter(F.col(col_name) != F.lit(col_value))

        logger.info(
            "Escrevendo DataFrame extraído para parquet: %s",
            target_file,
        )
        df.write.mode("overwrite").parquet(target_file)
        return target_file

    @log_operation
    def extract_cdr_algar_huawei(self, source_file: str, target_file: str) -> str:
        """Extrai registros do layout Algar Huawei usando o contrato pré-configurado.

        Args:
            source_file: Caminho do arquivo de entrada no formato Algar Huawei.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            str: Caminho do diretório Parquet persistido em ``target_file``.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_algar_huawei(
            ...     source_file="dados/algar_huawei.csv",
            ...     target_file="parquet/algar_huawei_extracted"
            ... )
        """
        return self.extract_cdr(source_file, target_file, self.schemas["algar_huawei"])
