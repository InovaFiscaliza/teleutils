"""Carga de Parquet em tabelas Iceberg compartilhadas com o Trino.

A escrita ocorre pelo Spark, não por uma conexão ao coordenador Trino. A sessão
deve ter as extensões Iceberg e acesso ao catálogo utilizado pelo destino.

Responsabilidades:
    - Ler o Parquet padronizado produzido pela etapa de transformação.
    - Verificar as colunas finais obrigatórias e a chave de identificação.
    - Deduplicar a origem e criar a tabela ou executar o MERGE INTO.

Dependências relevantes:
    - pyspark.sql.SparkSession: leitura, catálogo e execução da carga.
    - teleutils._config: contrato TARGET_SCHEMA e PRIMARY_KEY_COLUMNS padrão.
    - teleutils._logging.log_operation: logs de início, sucesso e falha.

Notes:
    Não há normalização de CDR, conversão de tipos ou renomeação de colunas.
    A carga não configura o catálogo nem verifica a conexão com o Trino.
    A tabela só será compartilhada se ambos acessarem o mesmo catálogo e dados.

Example:
    >>> loader = TrinoLoader(spark)
    >>> loader.upsert_iceberg("/dados/cdr_padronizado", "catalogo.schema.chamadas")
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from uuid import uuid4

from pyspark.sql import SparkSession

from teleutils._config import PRIMARY_KEY_COLUMNS, TARGET_SCHEMA
from teleutils._logging import log_operation

logger = logging.getLogger(__name__)


def _quote_identifier(identifier: str) -> str:
    """Delimita um identificador Spark SQL e escapa suas crases internas.

    O argumento representa um único nome literal, não um caminho qualificado.
    Pontos no argumento permanecem parte do identificador delimitado.
    """
    return "`" + identifier.replace("`", "``") + "`"


class TrinoLoader:
    """Carrega dados padronizados em tabelas Iceberg consultadas pelo Trino.

    Atua após a transformação dos CDRs, consumindo os nomes finais de colunas
    definidos em TARGET_SCHEMA. A sessão recebida é reutilizada para leitura,
    consulta ao catálogo, criação de views temporárias e persistência.

    Attributes:
        spark: Sessão Spark configurada com extensões e catálogo Iceberg.

    Notes:
        A classe não cria nem encerra a sessão e não valida sua configuração
        durante a inicialização. O catálogo deve suportar as operações Iceberg
        utilizadas por upsert_iceberg.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Armazena a sessão recebida sem modificar sua configuração.

        Args:
            spark: Sessão compartilhada com o pipeline e configurada pelo
                chamador para acessar o catálogo Iceberg de destino.

        Notes:
            Não há leitura de Parquet nem acesso à tabela neste construtor.
        """
        self.spark = spark

    @log_operation
    def upsert_iceberg(
        self,
        source_file: str,
        target_table: str,
        primary_keys: Sequence[str] = PRIMARY_KEY_COLUMNS,
    ) -> None:
        """Lê Parquet e insere ou atualiza registros pela chave composta.

        Fluxo de processamento:
            1. Valida os nomes da chave e a estrutura do nome da tabela.
            2. Lê o Parquet e verifica as colunas finais e as chaves solicitadas.
            3. Deduplica a origem considerando somente primary_keys.
            4. Cria a tabela Iceberg ausente com os dados deduplicados ou
               executa MERGE na tabela existente por uma view temporária.

        Args:
            source_file: Caminho do arquivo ou diretório Parquet padronizado.
            target_table: Nome do destino, preferencialmente catalogo.schema.tabela,
                com componentes sem delimitadores SQL. Aceita de um a três
                componentes separados por pontos, nenhum deles vazio ou composto
                apenas por espaços. Cada componente é delimitado em crases.
            primary_keys: Sequência não vazia de nomes de colunas existentes,
                sem repetições nem nomes vazios ou compostos apenas por espaços.
                Usa PRIMARY_KEY_COLUMNS por padrão e copia a sequência antes
                de utilizá-la, sem modificar o objeto recebido.

        Returns:
            None: Os dados são persistidos na tabela; não retorna DataFrame
            nem contagem de registros inseridos ou atualizados.

        Raises:
            ValueError: Se o destino ou a chave forem inválidos ou faltarem
                colunas finais de TARGET_SCHEMA ou da chave no Parquet.

        Notes:
            Todas as colunas finais de TARGET_SCHEMA devem estar presentes no
            Parquet. Colunas adicionais são permitidas; tipos não são validados.
            A validação considera o primeiro elemento de cada tupla do contrato,
            como nu_referencia, não a chave referencia do dicionário. Os nomes
            são comparados exatamente, sem normalização de maiúsculas ou espaços.

            O catálogo Spark deve apontar para a mesma tabela acessada pelo
            Trino. A tabela ausente é criada usando Iceberg, sem substituição.
            O schema usado na criação é o do DataFrame lido, incluindo colunas
            adicionais. Não há particionamento explícito na criação da tabela.
            A existência é consultada antes da escrita; criação concorrente por
            outro processo não é tratada por este método.

            A origem é deduplicada pela chave; se registros da mesma chave
            divergirem, não há garantia de qual registro será mantido.
            Chaves nulas são comparadas com igualdade null-safe no MERGE.
            A correspondência exige igualdade em todas as colunas da chave.
            Registros correspondentes são atualizados; os demais são inseridos.
            Não há remoção de registros do destino nem criação de restrição de
            chave primária. A deduplicação afeta apenas a origem desta chamada.
            UPDATE SET * e INSERT * pressupõem schemas compatíveis entre origem
            e destino. Não são reaplicadas as transformações de CDR.

            dropDuplicates constrói um plano lazy; create() ou a execução do
            MERGE materializam o processamento distribuído e a persistência.
            O caminho de MERGE registra uma view local com sufixo UUID na sessão
            recebida e tenta removê-la no finally, inclusive se o SQL falhar.
            A remoção não possui tratamento próprio de exceções.

            log_operation registra início, sucesso ou falha e relança exceções.
            Não há retry nem recuperação de falhas de leitura, catálogo ou carga.
            A configuração dos handlers de logging cabe à aplicação consumidora.

            Anotação de manutenção: alterações nos nomes finais de TARGET_SCHEMA
            impactam a validação de todo Parquet, mesmo com chaves personalizadas.
            Alterações em PRIMARY_KEY_COLUMNS impactam a deduplicação e o ON
            quando o argumento primary_keys não é fornecido.
        """
        keys = list(primary_keys)
        if not keys or any(not isinstance(key, str) or not key.strip() for key in keys):
            raise ValueError("primary_keys deve conter nomes de colunas não vazios.")
        if len(keys) != len(set(keys)):
            raise ValueError("primary_keys não deve conter colunas repetidas.")

        table_parts = target_table.split(".")
        if not 1 <= len(table_parts) <= 3 or any(
            not part.strip() for part in table_parts
        ):
            raise ValueError("target_table deve identificar uma tabela válida.")
        quoted_table = ".".join(_quote_identifier(part) for part in table_parts)

        logger.info("Lendo Parquet para carga em %s: %s", target_table, source_file)
        df = self.spark.read.parquet(source_file)
        missing_columns = [
            column for column, _ in TARGET_SCHEMA.values() if column not in df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Colunas finais de TARGET_SCHEMA ausentes no Parquet: {missing_columns}"
            )
        missing_keys = [key for key in keys if key not in df.columns]
        if missing_keys:
            raise ValueError(f"Colunas da chave ausentes no Parquet: {missing_keys}")

        df_deduped = df.dropDuplicates(keys)
        if not self.spark.catalog.tableExists(quoted_table):
            logger.info(
                "Criando tabela Iceberg e inserindo registros: %s", target_table
            )
            df_deduped.writeTo(quoted_table).using("iceberg").create()
            return

        temp_view_name = f"stg_source_updates_{uuid4().hex}"
        join_condition = " AND ".join(
            f"target.{_quote_identifier(key)} <=> source.{_quote_identifier(key)}"
            for key in keys
        )
        df_deduped.createOrReplaceTempView(temp_view_name)
        try:
            logger.info("Atualizando e inserindo registros em %s", target_table)
            self.spark.sql(
                f"""
                MERGE INTO {quoted_table} AS target
                USING {_quote_identifier(temp_view_name)} AS source
                ON {join_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            self.spark.catalog.dropTempView(temp_view_name)
