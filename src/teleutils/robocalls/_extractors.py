import logging
from dataclasses import dataclass
from functools import wraps
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


@dataclass
class CDRSchema:
    """
    Representa a configuração de leitura e mapeamento de colunas de um arquivo CDR.

    Usando dataclass aqui para agrupar configuração de forma limpa e imutável,
    sem precisar de boilerplate de __init__ e __repr__.
    """

    name: str  # Nome do formato (ex: "Ericsson", "TIM VoLTE")
    delimiter: str  # Delimitador do CSV
    has_header: bool  # Se o arquivo possui cabeçalho
    column_indices: list[int]  # Índices das colunas a selecionar
    column_names: list[str]  # Nomes finais das colunas (na mesma ordem dos índices)
    job_description: str  # Descrição do job para monitoramento no Spark UI


def log_extraction(method):
    """
    Decorador para registrar o início e fim de cada extração.

    Decoradores são ideais aqui pois aplicam comportamento transversal
    (logging) sem poluir a lógica principal de cada método.
    """

    @wraps(method)
    def wrapper(self, source_file: str, *args, **kwargs) -> DataFrame:
        logger.info("Iniciando extração [%s]: %s", method.__name__, source_file)
        try:
            result = method(self, source_file, *args, **kwargs)
            logger.info("Extração [%s] concluída com sucesso.", method.__name__)
            return result
        except Exception as e:
            logger.exception("Falha na extração [%s]: %s", method.__name__, e)
            raise

    return wrapper


class RoboCallsExtractor:
    """
    Extrai e padroniza registros de chamadas (CDR) de diferentes operadoras/formatos.

    A lógica de leitura foi centralizada no método privado `_extract_cdr`,
    enquanto os métodos públicos apenas declaram a configuração do schema.
    Isso segue o princípio DRY e facilita adicionar novos formatos no futuro.
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
        self.spark = spark
        # SparkContext armazenado uma única vez, evitando chamadas repetidas
        self._sc = spark.sparkContext

    def _extract_cdr(self, source_file: str, schema: CDRSchema) -> DataFrame:
        """
        Lógica central de extração, reutilizada por todos os métodos públicos.

        Separar a lógica de negócio (o que fazer) da configuração (como fazer)
        é um princípio fundamental de design. Novos formatos são adicionados
        apenas incluindo um novo CDRSchema — sem duplicar código.
        """
        self._sc.setJobDescription(schema.job_description)

        df = (
            self.spark.read.format("csv")
            .option("delimiter", schema.delimiter)
            .option("header", schema.has_header)
            .load(source_file)
        )

        # Valida se todos os índices solicitados existem no DataFrame lido.
        # Falhar cedo com mensagem clara é melhor do que erros crípticos do Spark.
        max_index = max(schema.column_indices)
        if max_index >= len(df.columns):
            raise ValueError(
                f"Schema '{schema.name}' requer coluna no índice {max_index}, "
                f"mas o arquivo possui apenas {len(df.columns)} colunas: {source_file}"
            )

        columns_to_keep = [df.columns[i] for i in schema.column_indices]
        return df.select(columns_to_keep).toDF(*schema.column_names)

    @log_extraction
    def extract_cdr_ericsson(self, source_file: str) -> DataFrame:
        """Extrai CDR no formato Ericsson."""
        return self._extract_cdr(source_file, self._SCHEMAS["ericsson"])

    @log_extraction
    def extract_cdr_tim_volte(self, source_file: str) -> DataFrame:
        """Extrai CDR no formato TIM VoLTE."""
        return self._extract_cdr(source_file, self._SCHEMAS["tim_volte"])

    @log_extraction
    def extract_cdr_tim_stir(self, source_file: str) -> DataFrame:
        """Extrai CDR no formato TIM STIR."""
        return self._extract_cdr(source_file, self._SCHEMAS["tim_stir"])

    @log_extraction
    def extract_cdr_vivo_volte(self, source_file: str) -> DataFrame:
        """Extrai CDR no formato Vivo VoLTE."""
        return self._extract_cdr(source_file, self._SCHEMAS["vivo_volte"])

    def extract_cdr(self, source_file: str, format_key: str) -> DataFrame:
        """
        Método genérico para extração por chave de formato.

        Permite chamar extractor.extract_cdr(path, "ericsson") programaticamente,
        útil quando o formato é determinado em tempo de execução (ex: lendo de config).
        """
        if format_key not in self._SCHEMAS:
            raise KeyError(
                f"Formato '{format_key}' não reconhecido. "
                f"Disponíveis: {list(self._SCHEMAS.keys())}"
            )
        return self._extract_cdr(source_file, self._SCHEMAS[format_key])
