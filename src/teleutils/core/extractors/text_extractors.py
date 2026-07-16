"""Módulo teleutils.core.extractors.text_extractors.

Responsável pela extração e padronização de registros de chamadas (CDR)
provenientes de múltiplos layouts de arquivo texto/CSV utilizados por
prestadoras e fornecedores distintos.

Principais responsabilidades:
    - Definir contratos de mapeamento por formato de CDR via ``CDRSchema``.
    - Centralizar a leitura e validação de colunas em Spark.
    - Uniformizar nomes de colunas em uma estrutura comum para etapas seguintes.
    - Persistir o resultado em parquet particionado por ``tipo_chamada``.

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
    Os índices de coluna em ``CDRSchema.column_indices`` são zero-based e devem
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
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._logging import log_operation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CDRSchema:
    """Representa a configuração de leitura e mapeamento para um layout de CDR.

    Esta dataclass concentra, em um único objeto, todas as decisões necessárias
    para interpretar um arquivo CDR de um formato específico. A ideia é separar
    configuração de execução: a classe ``CDRTextExtractor`` apenas aplica esse
    contrato, enquanto cada instância de ``CDRSchema`` define as regras.

    Attributes:
        name: Nome descritivo do formato CDR (ex.: "Ericsson", "TIM VoLTE").
        delimiter: Delimitador utilizado no arquivo de entrada (ex.: ";", "|").
        schema: Schema Spark opcional para leitura quando não há cabeçalho
            confiável.
        has_header: Indica se o arquivo de entrada possui linha de cabeçalho.
        column_to_filter: Regra opcional de filtro após seleção/renomeação, no
            formato ``(nome_coluna, valor)``.
        column_indices: Lista de índices de colunas no arquivo de origem.
        column_names: Lista de nomes finais na saída, na mesma ordem dos índices.
        job_description: Texto de descrição do job exibido na Spark UI.

    Notes:
        A classe é imutável (``frozen=True``), reduzindo risco de alterações
        acidentais de configuração durante a execução.

        O método ``__post_init__`` realiza validações de integridade para evitar
        inconsistências de mapeamento, como tamanho divergente entre
        ``column_indices`` e ``column_names``.

    Example:
        >>> schema = CDRSchema(
        ...     name="Ericsson",
        ...     delimiter=";",
        ...     schema=None,
        ...     has_header=True,
        ...     column_to_filter=None,
        ...     column_indices=[0, 1, 2, 3, 4, 9, 11],
        ...     column_names=[
        ...         "referencia",
        ...         "numero_origem",
        ...         "_data",
        ...         "_hora",
        ...         "tipo_chamada",
        ...         "numero_destino",
        ...         "duracao",
        ...     ],
        ...     job_description="Extraindo CDR: Ericsson",
        ... )
    """

    name: str  # Nome do formato (ex: "Ericsson", "TIM VoLTE")
    delimiter: str  # Delimitador do CSV
    schema: (
        T.StructType | None
    )  # Esquema de leitura opcional para arquivos sem cabeçalho ou com cabeçalho inconsistente
    has_header: bool  # Se o arquivo possui cabeçalho
    column_to_filter: (
        tuple[str, str] | None
    )  # Tupla (nome_coluna, valor) para filtrar linhas, ou None para não filtrar
    column_indices: list[int]  # Índices das colunas a selecionar
    column_names: list[str]  # Nomes finais das colunas (na mesma ordem dos índices)
    job_description: str  # Descrição do job para monitoramento no Spark UI

    def __post_init__(self) -> None:
        """Valida consistência interna da configuração após inicialização.

        Este método impede que configurações inválidas avancem para a etapa de
        extração, onde erros seriam mais caros de diagnosticar.

        Raises:
            ValueError: Quando ``schema`` não é ``StructType``/``None``.
            ValueError: Quando ``column_to_filter`` não segue ``(str, str)``.
            ValueError: Quando a coluna em ``column_to_filter`` não existe em
                ``column_names``.
            ValueError: Quando ``column_indices`` e ``column_names`` têm tamanhos
                diferentes.
            ValueError: Quando ``column_indices`` está vazio ou contém índice
                negativo.
            ValueError: Quando algum índice requerido excede a quantidade de
                colunas declaradas no ``schema`` informado.

        Notes:
            As validações aqui são parte da estratégia de "falhar cedo",
            simplificando manutenção e reduzindo falhas em pipelines longos.
        """
        if self.schema is not None and not isinstance(self.schema, T.StructType):
            raise ValueError(
                f"Schema '{self.name}': schema deve ser None ou um StructType. "
                f"Recebido: {type(self.schema).__name__}"
            )
        if self.column_to_filter is not None:
            if (
                not isinstance(self.column_to_filter, tuple)
                or len(self.column_to_filter) != 2
                or not all(isinstance(v, str) for v in self.column_to_filter)
            ):
                raise ValueError(
                    f"Schema '{self.name}': column_to_filter deve ser None ou uma "
                    f"tupla de duas strings. Recebido: {self.column_to_filter!r}"
                )
            col_name, _ = self.column_to_filter
            if col_name not in self.column_names:
                raise ValueError(
                    f"Schema '{self.name}': coluna '{col_name}' em column_to_filter "
                    f"não está presente em column_names: {self.column_names}"
                )
        if len(self.column_indices) != len(self.column_names):
            raise ValueError(
                f"Schema '{self.name}': column_indices tem "
                f"{len(self.column_indices)} elemento(s), mas column_names tem "
                f"{len(self.column_names)}. Devem ter o mesmo tamanho."
            )
        if not self.column_indices:
            raise ValueError(
                f"Schema '{self.name}': column_indices não pode ser vazio."
            )
        if any(i < 0 for i in self.column_indices):
            raise ValueError(
                f"Schema '{self.name}': índices negativos não são permitidos. "
                f"Recebido: {self.column_indices}"
            )
        if self.schema is not None and max(self.column_indices) >= len(self.schema):
            raise ValueError(
                f"Schema '{self.name}': schema possui {len(self.schema)} campo(s), "
                f"mas column_indices requer o índice {max(self.column_indices)}."
            )


class CDRTextExtractor:
    """Orquestra a extração de CDR para um formato intermediário padronizado.

    Esta classe funciona como ponto de entrada para extração por tecnologia/
    fornecedor. Cada método público seleciona um esquema pré-definido e delega a
    execução para ``_extract_cdr``, onde está o fluxo comum de processamento.

    O desenho separa lógica (implementação da extração) de configuração
    (mapeamentos em ``_SCHEMAS``), facilitando evolução e manutenção incremental.

    Attributes:
        spark: Sessão Spark utilizada para leitura e escrita de dados.
        _sc: Referência ao SparkContext para definição de descrições de job.

    Notes:
        Ponto de extensão principal: adição de novos formatos no dicionário
        ``_SCHEMAS`` e criação de um método público delegando para ``_extract_cdr``.
    """

    # Schemas declarados como atributo de classe: são constantes e não dependem
    # de instância. Isso evita recriar os objetos a cada chamada e deixa a
    # configuração visível e fácil de manter no topo da classe.
    _SCHEMAS: dict[str, CDRSchema] = {
        "ericsson": CDRSchema(
            name="Ericsson",
            delimiter=";",
            schema=None,
            has_header=True,
            column_to_filter=None,
            column_indices=[0, 1, 2, 3, 4, 8, 9, 11, 20],
            column_names=[
                "referencia",
                "numero_origem",
                "_data",
                "_hora",
                "_tipo_chamada",
                "rota_saida",
                "numero_destino",
                "duracao",
                "rota_entrada",
            ],
            job_description="Extraindo CDR: Ericsson",
        ),
        "tim_huawei": CDRSchema(
            name="Tim Huawei",
            delimiter=";",
            schema=T.StructType(
                [T.StructField(f"_c{i}", T.StringType(), True) for i in range(17)]
            ),
            has_header=False,
            column_to_filter=("_tipo_chamada", "TipodeCDR(role-of-Node)"),
            column_indices=[0, 1, 2, 3, 4, 7, 12, 16],
            column_names=[
                "numero_origem",
                "_data",
                "_hora",
                "_tipo_chamada",
                "numero_destino",
                "duracao",
                "referencia",
                "_autenticacao",
            ],
            job_description="Extraindo CDR: Tim Huawei",
        ),
        "vivo_fcdr": CDRSchema(
            name="Vivo FCDR",
            delimiter="|",
            schema=None,
            has_header=False,
            column_to_filter=None,
            column_indices=[0, 2, 5, 12, 13, 31, 45],
            column_names=[
                "_tipo_chamada",
                "_numero_origem",
                "numero_destino",
                "duracao",
                "_data",
                "_hora",
                "referencia",
            ],
            job_description="Extraindo CDR: Vivo FCDR",
        ),
        "claro_nokia": CDRSchema(
            name="Claro Nokia",
            delimiter=";",
            schema=None,
            has_header=True,
            column_to_filter=None,
            column_indices=[0, 2, 3, 7, 8, 13, 15, 21],
            column_names=[
                "_tipo_chamada",
                "_referencia",
                "data_hora",
                "numero_origem",
                "numero_destino",
                "numero_conectado",
                "duracao",
                "_rota",
            ],
            job_description="Extraindo CDR: Claro Nokia",
        ),
    }

    def __init__(self, spark: SparkSession) -> None:
        """Inicializa o extrator com uma sessão Spark ativa.

        Args:
            spark: Sessão Spark a ser reutilizada nas operações de extração.

        Notes:
            A referência de ``spark.sparkContext`` é armazenada para evitar acesso
            repetitivo e para permitir instrumentação de jobs no Spark UI.
        """
        self.spark = spark
        # SparkContext armazenado uma única vez, evitando chamadas repetidas
        # self._sc = spark.sparkContext

    def _extract_cdr(
        self, source_file: str, target_file: str, schema: CDRSchema
    ) -> DataFrame:
        """Executa o pipeline de extração/normalização para um esquema CDR.

        Fluxo de processamento:
            1. Define descrição do job para observabilidade no Spark UI.
            2. Lê o CSV conforme delimitador/cabeçalho/schema informados.
            3. Valida existência dos índices solicitados no dataset lido.
            4. Seleciona e renomeia colunas para o contrato padronizado.
            5. Adiciona metadados de linhagem (prestadora, tipo_cdr, arquivo_origem).
            6. Aplica filtro opcional definido no schema.
            7. Persiste parquet particionado por ``tipo_chamada``.

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
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

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
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["tim_huawei"])

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
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["vivo_fcdr"])

    @log_operation
    def extract_cdr_claro_nokia(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai registros CDR no layout Claro Nokia.

        Args:
            source_file: Caminho do arquivo de entrada no formato Claro Nokia.
            target_file: Diretório de saída em parquet padronizado.

        Returns:
            DataFrame: Registros extraídos e normalizados do formato Claro Nokia.

        Raises:
            ValueError: Se o arquivo não obedecer o layout esperado pelo schema.
            Exception: Erros propagados do pipeline Spark.

        Example:
            >>> extrator = CDRTextExtractor(spark)
            >>> df = extrator.extract_cdr_claro_nokia(
            ...     source_file="dados/claro_nokia.csv",
            ...     target_file="parquet/claro_nokia_extracted"
            ... )
        """
        df = self._extract_cdr(source_file, target_file, self._SCHEMAS["claro_nokia"])

        # Corrige uma coluna referência (hexadecimal BCD invertida) no formato WORD:WORD:BYTE.
        # Exemplo: 'C407FEF101' -> '704C1FEF10'
        df = df.withColumn(
            "referencia",  # Sobrescreve a coluna original
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
        )

        return df
