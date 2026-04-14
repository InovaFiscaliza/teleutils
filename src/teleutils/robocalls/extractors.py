"""Extração e padronização de registros de chamadas (CDR) de múltiplos formatos.

Este módulo fornece ferramentas para ler arquivos de CDR (Call Detail Records) de
diferentes operadoras telefônicas e formatos de tecnologia, extraindo as colunas
relevantes e normalizando seus nomes. Suporta formatos de fabricantes diversos
(Ericsson, TIM VoLTE, TIM STIR, Vivo VoLTE) através de um mecanismo baseado em
esquemas de mapeamento.

A extração é centralizada no método privado `_extract_cdr`, que padroniza a
lógica de leitura, validação de índices de coluna e escrita particionada. Novos
formatos podem ser adicionados declarando um novo `CDRSchema` no dicionário de
classe `_SCHEMAS`, sem necessidade de duplicar código.

Nota:
    Os arquivos extraídos são salvos como parquet particionado por tipo_de_chamada.
    Os índices de coluna são baseados em zero e devem corresponder ao layout do
    arquivo CSV de entrada após aplicação do delimitador configurado.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from teleutils._logging import log_operation

logger = logging.getLogger(__name__)

MAX_RECORDS_PER_FILE = 1000000  # Limite de registros por arquivo parquet para evitar arquivos muito grandes


@dataclass(frozen=True)
class CDRSchema:
    """Configura o mapeamento de colunas para extração de um formato CDR específico.

    Uma dataclass imutável que agrupa a configuração necessária para ler e mapear
    colunas de um arquivo CDR. Cada formato de CDR (Ericsson, TIM VoLTE, etc.)
    possui um esquema distinto que especifica o delimitador, presença de cabeçalho,
    quais colunas do arquivo original devem ser selecionadas e seus nomes finais.

    Atributos:
        name (str): Nome descritivo do formato CDR (ex: "Ericsson", "TIM VoLTE").
        delimiter (str): Caractere delimitador usado no arquivo CSV
            (ex: ";" ou "|").
        has_header (bool): Indica se o arquivo contém uma linha de cabeçalho.
        column_indices (list[int]): Índices das colunas do arquivo original a serem
            selecionadas, em ordem zero-indexada. Exemplo: [0, 1, 2, 3, 4, 9, 11].
        column_names (list[str]): Nomes das colunas no DataFrame de saída,
            na mesma ordem dos índices selecionados.
            Exemplo: ["referencia", "numero_de_a", "_data", "_hora", ...].
        job_description (str): Descrição do job para monitoramento no Spark UI.

    Nota:
        A dataclass é congelada (frozen=True), garantindo imutabilidade.
        O método `__post_init__` valida automaticamente que:
        - `skip_rows` não é negativo
        - `column_indices` e `column_names` têm o mesmo tamanho
        - `column_indices` não está vazio
        - Nenhum índice é negativo

    Exemplo:
        >>> schema = CDRSchema(
        ...     name="Ericsson",
        ...     delimiter=";",
        ...     has_header=True,
        ...     skip_rows=0,
        ...     column_indices=[0, 1, 2, 3, 4, 9, 11],
        ...     column_names=["referencia", "numero_de_a", "_data", "_hora",
        ...                   "tipo_de_chamada", "numero_de_b", "duracao_da_chamada"],
        ...     job_description="Extraindo CDR: Ericsson"
        ... )
    """

    name: str  # Nome do formato (ex: "Ericsson", "TIM VoLTE")
    delimiter: str  # Delimitador do CSV
    has_header: bool  # Se o arquivo possui cabeçalho
    column_to_filter: (
        tuple[str, str] | None
    )  # Tupla (nome_coluna, valor) para filtrar linhas, ou None para não filtrar, necessário para excluir linhas de cabeçalho mal formatadas em alguns arquivos
    column_indices: list[int]  # Índices das colunas a selecionar
    column_names: list[str]  # Nomes finais das colunas (na mesma ordem dos índices)
    job_description: str  # Descrição do job para monitoramento no Spark UI

    def __post_init__(self):
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


class RoboCallsExtractor:
    """Extrai e padroniza registros de chamadas (CDR) de múltiplas operadoras.

    Responsável por ler arquivos CDR em diferentes formatos, validar suas estruturas
    e exportá-los em um formato intermediário normalizado, parquet com particionamento
    por tipo_de_chamada.

    A classe mantém um dicionário de esquemas (_SCHEMAS) que definem como cada formato
    deve ser lido. A lógica de extração é centralizada no método privado `_extract_cdr`,
    que é reutilizado por todos os métodos públicos específicos de formato. Essa
    separação entre lógica e configuração (padrão de design Strategy) facilita a
    manutenção e a adição de novos formatos.

    Atributos:
        spark (SparkSession): Sessão Spark usada para leitura e escrita de dados.
    """

    # Schemas declarados como atributo de classe: são constantes e não dependem
    # de instância. Isso evita recriar os objetos a cada chamada e deixa a
    # configuração visível e fácil de manter no topo da classe.
    _SCHEMAS: dict[str, CDRSchema] = {
        "ericsson": CDRSchema(
            name="Ericsson",
            delimiter=";",
            has_header=True,
            column_to_filter=None,
            column_indices=[0, 1, 2, 3, 4, 9, 11],
            column_names=[
                "referencia",
                "numero_de_a",
                "_data",
                "_hora",
                "tipo_de_chamada",
                "numero_de_b",
                "duracao_da_chamada",
            ],
            job_description="Extraindo CDR: Ericsson",
        ),
        "tim_volte": CDRSchema(
            name="Tim VoLTE",
            delimiter=";",
            has_header=True,
            column_to_filter=("tipo_de_chamada", "TipodeCDR(role-of-Node)"),
            column_indices=[0, 1, 2, 3, 4, 7, 12, 16],
            column_names=[
                "numero_de_a",
                "_data",
                "_hora",
                "tipo_de_chamada",
                "numero_de_b",
                "duracao_da_chamada",
                "referencia",
                "autenticacao",
            ],
            job_description="Extraindo CDR: Tim VoLTE",
        ),
        "tim_stir": CDRSchema(
            name="Tim Stir",
            delimiter=";",
            has_header=True,
            column_to_filter=None,
            column_indices=[0, 1, 2, 5, 6, 11, 13, 14],
            column_names=[
                "numero_de_a",
                "_data",
                "_hora",
                "tipo_de_chamada",
                "referencia",
                "numero_de_b",
                "duracao_da_chamada",
                "autenticacao",
            ],
            job_description="Extraindo CDR: Tim Stir",
        ),
        "vivo_volte": CDRSchema(
            name="Vivo VoLTE",
            delimiter="|",
            has_header=False,
            column_to_filter=None,
            column_indices=[0, 2, 5, 12, 13, 31, 45],
            column_names=[
                "tipo_de_chamada",
                "numero_de_a",
                "numero_de_b",
                "duracao_da_chamada",
                "_data",
                "_hora",
                "referencia",
            ],
            job_description="Extraindo CDR: Vivo VoLTE",
        ),
    }

    def __init__(self, spark: SparkSession) -> None:
        """Inicializa o extrator com uma sessão Spark.

        Parâmetros:
            spark (SparkSession): Sessão Spark ativa para operações de I/O.
        """
        self.spark = spark
        # SparkContext armazenado uma única vez, evitando chamadas repetidas
        self._sc = spark.sparkContext

    def _extract_cdr(
        self, source_file: str, target_file: str, schema: CDRSchema
    ) -> DataFrame:
        """Extrai CDR de um arquivo CSV, validando e mapeando colunas conforme esquema.

        Método central que implementa a lógica de extração reutilizada por todo recurso
        específico de formato. Realiza as seguintes etapas:
        1. Define descrição do job no Spark para monitoramento
        2. Lê o arquivo CSV com delimitador e configuração de cabeçalho especificados
        3. Valida que todos os índices de coluna solicitados existem no arquivo
        4. Seleciona e renomeia as colunas conforme o esquema
        5. Grava o resultado como parquet, particionado por tipo_de_chamada

        Parâmetros:
            source_file (str): Caminho para o arquivo CSV de entrada.
            target_file (str): Caminho para o diretório parquet de saída.
            schema (CDRSchema): Esquema que define o mapeamento de colunas.

        Retorna:
            DataFrame: DataFrame do Spark contendo os registros extraídos e renomeados.

        Lança:
            ValueError: Se algum índice em schema.column_indices exceder o número de
                colunas do arquivo CSV, ou se o delimitador não estiver correto.
            FileNotFoundError: Se o arquivo de entrada não existir.
            Exception: Qualquer erro lançado pelo Spark ao ler ou gravar parquet.

        Nota:
            O particionamento por tipo_de_chamada no arquivo de saída melhora a velocidade
            de leitura em operações subsequentes que filtrem por esse campo.
            Falhas de validação são capturadas cedo com mensagens descritivas,
            facilitando diagnóstico de problemas de configuração ou formato.
        """
        self._sc.setJobDescription(schema.job_description)

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
                f"para o arquivo: {source_file}"
            )

        logger.info(
            "Selecionando e renomeando colunas conforme o esquema '%s'", schema.name
        )
        columns_to_keep = [df.columns[i] for i in schema.column_indices]
        df = df.select(columns_to_keep).toDF(*schema.column_names)

        if schema.column_to_filter is not None:
            col_name, col_value = schema.column_to_filter
            logger.info(
                "Aplicando filtro: %s = '%s' para o esquema '%s'",
                col_name,
                col_value,
                schema.name,
            )
            df = df.filter(df[col_name] != col_value)

        logger.info(
            "Escrevendo DataFrame extraído para parquet particionado por 'tipo_de_chamada': %s",
            target_file,
        )
        df.repartition("tipo_de_chamada").write.mode("overwrite").partitionBy(
            "tipo_de_chamada"
        ).option("maxRecordsPerFile", MAX_RECORDS_PER_FILE).parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR no formato Ericsson.

        Parâmetros:
            source_file (str): Caminho para o arquivo CSV Ericsson de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Retorna:
            DataFrame: DataFrame contendo os registros Ericsson extraídos.

        Exemplo:
            >>> extrator = RoboCallsExtractor(spark)
            >>> df = extrator.extract_cdr_ericsson(
            ...     source_file="dados/ericsson.csv",
            ...     target_file="parquet/ericsson_extracted"
            ... )
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["ericsson"])

    @log_operation
    def extract_cdr_tim_volte(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR no formato TIM VoLTE.

        Parâmetros:
            source_file (str): Caminho para o arquivo CSV TIM VoLTE de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Retorna:
            DataFrame: DataFrame contendo os registros TIM VoLTE extraídos.

        Exemplo:
            >>> extrator = RoboCallsExtractor(spark)
            >>> df = extrator.extract_cdr_tim_volte(
            ...     source_file="dados/tim_volte.csv",
            ...     target_file="parquet/tim_volte_extracted"
            ... )
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["tim_volte"])

    @log_operation
    def extract_cdr_tim_stir(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR no formato TIM STIR.

        Parâmetros:
            source_file (str): Caminho para o arquivo CSV TIM STIR de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Retorna:
            DataFrame: DataFrame contendo os registros TIM STIR extraídos.

        Exemplo:
            >>> extrator = RoboCallsExtractor(spark)
            >>> df = extrator.extract_cdr_tim_stir(
            ...     source_file="dados/tim_stir.csv",
            ...     target_file="parquet/tim_stir_extracted"
            ... )
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["tim_stir"])

    @log_operation
    def extract_cdr_vivo_volte(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR no formato Vivo VoLTE.

        Parâmetros:
            source_file (str): Caminho para o arquivo CSV Vivo VoLTE de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Retorna:
            DataFrame: DataFrame contendo os registros Vivo VoLTE extraídos.

        Exemplo:
            >>> extrator = RoboCallsExtractor(spark)
            >>> df = extrator.extract_cdr_vivo_volte(
            ...     source_file="dados/vivo_volte.csv",
            ...     target_file="parquet/vivo_volte_extracted"
            ... )
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["vivo_volte"])
