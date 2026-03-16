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

Note:
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


@dataclass(frozen=True)
class CDRSchema:
    """Configura o mapeamento de colunas para extração de um formato CDR específico.

    Uma dataclass imutável que agrupa a configuração necessária para ler e mapear
    colunas de um arquivo CDR. Cada formato de CDR (Ericsson, TIM VoLTE, etc.)
    possui um esquema distinto que especifica o delimitador, presença de cabeçalho,
    quais colunas do arquivo original devem ser selecionadas e seus nomes finais.

    Attributes:
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

    Note:
        A dataclass é congelada (frozen=True), garantindo imutabilidade.
        O método `__post_init__` valida automaticamente que:
        - `column_indices` e `column_names` têm o mesmo tamanho
        - `column_indices` não está vazio
        - Nenhum índice é negativo

    Example:
        >>> schema = CDRSchema(
        ...     name="Ericsson",
        ...     delimiter=";",
        ...     has_header=True,
        ...     column_indices=[0, 1, 2, 3, 4, 9, 11],
        ...     column_names=["referencia", "numero_de_a", "_data", "_hora",
        ...                   "tipo_de_chamada", "numero_de_b", "duracao_da_chamada"],
        ...     job_description="Extraindo CDR: Ericsson"
        ... )
    """

    name: str  # Nome do formato (ex: "Ericsson", "TIM VoLTE")
    delimiter: str  # Delimitador do CSV
    has_header: bool  # Se o arquivo possui cabeçalho
    column_indices: list[int]  # Índices das colunas a selecionar
    column_names: list[str]  # Nomes finais das colunas (na mesma ordem dos índices)
    job_description: str  # Descrição do job para monitoramento no Spark UI

    def __post_init__(self):
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

    Attributes:
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
            column_indices=[0, 1, 2, 3, 4, 7, 12],
            column_names=[
                "numero_de_a",
                "_data",
                "_hora",
                "tipo_de_chamada",
                "numero_de_b",
                "duracao_da_chamada",
                "referencia",
            ],
            job_description="Extraindo CDR: Tim VoLTE",
        ),
        "tim_stir": CDRSchema(
            name="Tim Stir",
            delimiter=";",
            has_header=True,
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

        Args:
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

        Args:
            source_file (str): Caminho para o arquivo CSV de entrada.
            target_file (str): Caminho para o diretório parquet de saída.
            schema (CDRSchema): Esquema que define o mapeamento de colunas.

        Returns:
            DataFrame: DataFrame do Spark contendo os registros extraídos e renomeados.

        Raises:
            ValueError: Se algum índice em schema.column_indices exceder o número de
                colunas do arquivo CSV, ou se o delimitador não estiver correto.
            FileNotFoundError: Se o arquivo de entrada não existir.
            Exception: Qualquer erro lançado pelo Spark ao ler ou gravar parquet.

        Note:
            O particionamento por tipo_de_chamada no arquivo de saída melhora a velocidade
            de leitura em operações subsequentes que filtrem por esse campo.
            Falhas de validação são capturadas cedo com mensagens descritivas,
            facilitando diagnóstico de problemas de configuração ou formato.
        """
        self._sc.setJobDescription(schema.job_description)

        df = (
            self.spark.read.format("csv")
            .option("delimiter", schema.delimiter)
            .option("header", schema.has_header)
            .option("inferSchema", False)
            .load(source_file)
        )

        # Valida se todos os índices solicitados existem no DataFrame lido.
        # Falhar cedo com mensagem clara é melhor do que erros crípticos do Spark.
        max_index = max(schema.column_indices)
        if max_index >= len(df.columns):
            raise ValueError(
                f"Schema '{schema.name}' requer coluna no índice {max_index}, "
                f"mas o arquivo possui apenas {len(df.columns)} colunas.\n"
                f"Verifique se o delimitador '{schema.delimiter}' está correto "
                f"para o arquivo: {source_file}"
            )

        columns_to_keep = [df.columns[i] for i in schema.column_indices]
        df = df.select(columns_to_keep).toDF(*schema.column_names)
        df.write.mode("overwrite").partitionBy("tipo_de_chamada").parquet(target_file)
        return self.spark.read.parquet(target_file)

    @log_operation
    def extract_cdr_ericsson(self, source_file: str, target_file: str) -> DataFrame:
        """Extrai CDR no formato Ericsson.

        Args:
            source_file (str): Caminho para o arquivo CSV Ericsson de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Returns:
            DataFrame: DataFrame contendo os registros Ericsson extraídos.

        Example:
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

        Args:
            source_file (str): Caminho para o arquivo CSV TIM VoLTE de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Returns:
            DataFrame: DataFrame contendo os registros TIM VoLTE extraídos.

        Example:
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

        Args:
            source_file (str): Caminho para o arquivo CSV TIM STIR de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Returns:
            DataFrame: DataFrame contendo os registros TIM STIR extraídos.

        Example:
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

        Args:
            source_file (str): Caminho para o arquivo CSV Vivo VoLTE de entrada.
            target_file (str): Caminho para o diretório parquet de saída.

        Returns:
            DataFrame: DataFrame contendo os registros Vivo VoLTE extraídos.

        Example:
            >>> extrator = RoboCallsExtractor(spark)
            >>> df = extrator.extract_cdr_vivo_volte(
            ...     source_file="dados/vivo_volte.csv",
            ...     target_file="parquet/vivo_volte_extracted"
            ... )
        """
        return self._extract_cdr(source_file, target_file, self._SCHEMAS["vivo_volte"])
