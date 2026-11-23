"""Carga de Parquet em tabelas Iceberg compartilhadas com o Trino.

Etapa final do pipeline: recebe o Parquet padronizado produzido pelos
transformers e o grava em uma tabela Iceberg. As consultas e escritas ocorrem
pelo Spark, não por conexão ao coordenador Trino. A sessão deve ter as
extensões Iceberg e acesso ao catálogo do destino.

Estratégia de carga (única, sem modos de operação):
    O valor de ``no_arquivo_origem`` identifica cada origem. Origens ainda
    ausentes no destino são apenas inseridas; origens já carregadas têm suas
    linhas excluídas do destino antes de a entrada ser inserida. Não há chaves
    primárias, ``MERGE`` nem tabela externa de controle.

Fluxo de processamento:
    Parquet de entrada
      ↓
    Validação das colunas finais de ``TARGET_SCHEMA`` e de ``no_arquivo_origem``
      ↓
    Origens distintas (persistidas e reutilizadas nas etapas seguintes)
      ↓
    Destino inexistente → criação com a entrada
    Destino existente   → consulta de existência → DELETE (se houver) → INSERT

Dependências relevantes:
    - ``teleutils._config.TARGET_SCHEMA``: colunas finais exigidas na entrada.
    - ``teleutils._logging.log_operation``: log de início, sucesso e falha.
    - ``pyspark``: leitura do Parquet, consultas SQL e escrita no Iceberg.

Notes:
    Não há transação conjunta entre ``DELETE`` e ``INSERT`` (commits separados).
    Os detalhes e o procedimento de recuperação estão em
    ``TrinoLoader.upsert_iceberg``.

Example:
    >>> loader = TrinoLoader(spark)
    >>> loader.upsert_iceberg("/dados/cdr_padronizado", "catalogo.schema.chamadas")
"""

from __future__ import annotations

import logging
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import TARGET_SCHEMA
from teleutils._logging import log_operation

logger = logging.getLogger(__name__)

# Coluna que identifica a origem de cada linha e controla a substituição.
_ORIGIN = "no_arquivo_origem"


def _quote_identifier(identifier: str) -> str:
    """Delimita um componente literal de identificador Spark SQL.

    Args:
        identifier: Nome de um único componente (catálogo, schema, tabela ou
            view temporária). Pontos permanecem parte do nome.

    Returns:
        O nome entre crases, com cada crase interna duplicada.
    """
    return "`" + identifier.replace("`", "``") + "`"


def _table_parts(name: str) -> list[str]:
    """Valida e divide o nome do destino em um a três componentes.

    Args:
        name: Nome da tabela, como ``catalogo.schema.tabela``. A divisão por
            pontos é literal: não há tratamento de crases nem de pontos
            escapados.

    Returns:
        Componentes do nome, sem normalização.

    Raises:
        TypeError: Se ``name`` não for string.
        ValueError: Se houver menos de um ou mais de três componentes, ou
            algum deles estiver vazio ou composto apenas por espaços.
    """
    if not isinstance(name, str):
        raise TypeError("target_table deve ser um nome de tabela.")
    parts = name.split(".")
    if not 1 <= len(parts) <= 3 or any(not part.strip() for part in parts):
        raise ValueError("target_table deve identificar uma tabela válida.")
    return parts


class TrinoLoader:
    """Carrega dados padronizados em tabelas Iceberg consultadas pelo Trino.

    Atua após a transformação dos CDRs, consumindo o Parquet com os nomes
    finais de colunas definidos em ``TARGET_SCHEMA``. Não normaliza dados,
    converte tipos nem renomeia colunas.

    Attributes:
        spark: Sessão Spark com extensões Iceberg e catálogo compartilhado com
            o Trino, usada para leitura, consultas e escrita.

    Notes:
        A classe não cria nem encerra a sessão e não valida sua configuração.
    """

    def __init__(self, spark: SparkSession) -> None:
        """Armazena a sessão recebida sem modificar sua configuração.

        Args:
            spark: Sessão compartilhada com o pipeline, configurada pelo
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
    ) -> None:
        """Insere o Parquet e substitui registros das origens já carregadas.

        Fluxo de processamento:
            1. Valida o nome do destino, lê o Parquet e confere as colunas
               finais de ``TARGET_SCHEMA`` e o tipo string de ``no_arquivo_origem``.
            2. Obtém as origens distintas (persistidas) e valida que não há
               valores nulos, vazios ou só com espaços. Entrada sem linhas
               encerra a carga sem acessar o destino.
            3. Destino inexistente: cria a tabela Iceberg com o schema da
               entrada e grava os dados.
            4. Destino existente: valida ``no_arquivo_origem`` no destino e
               consulta, por uma view temporária das origens, se alguma já está
               carregada.
            5. Havendo correspondência, confere a compatibilidade de colunas e
               tipos e executa ``DELETE`` somente das linhas dessas origens.
               Sem correspondência, nenhum ``DELETE`` é executado.
            6. Insere todas as linhas do Parquet (``append``) somente após a
               conclusão do ``DELETE``.

        Args:
            source_file: Arquivo ou diretório Parquet com as colunas finais de
                TARGET_SCHEMA e todas as linhas de cada origem nele presente.
            target_table: Destino Iceberg, preferencialmente catalogo.schema.tabela,
                com componentes sem delimitadores SQL.

        Raises:
            ValueError: Se houver nome de destino inválido, colunas ausentes,
                origem nula/vazia, coluna ``no_arquivo_origem`` ausente no
                destino ou, em recargas, colunas diferentes das do destino.
            TypeError: Se o destino não for um nome de tabela ou se a coluna
                no_arquivo_origem não for string no Parquet ou no destino.

        Notes:
            Cada nome identifica uma única origem por tabela. Entradas vazias
            com schema válido são ignoradas. Um destino ausente é criado com o
            schema do Parquet. Não há deduplicação dos registros inseridos.

            A consulta de existência e o ``DELETE`` filtram o destino pelas
            origens da entrada sem coletá-las no driver; ambos acessam o
            destino, e o custo depende de layout, metadados e configuração
            Iceberg. A verificação de compatibilidade usa ``DataFrame.to`` apenas
            para análise do schema; o DataFrame gravado é o original. Sem
            correspondência, a compatibilidade fica a cargo do ``append``.

            DELETE e INSERT são commits separados. Serialize as escritas no
            destino e mantenha o Parquet estável. Se a inserção falhar após a
            exclusão, verifique o estado do destino e reprocesse a mesma entrada
            completa; não há rollback automático. O ``log_operation`` registra
            e relança qualquer falha.
        """
        quoted_table = ".".join(
            _quote_identifier(part) for part in _table_parts(target_table)
        )
        logger.info("Lendo Parquet para carga em %s: %s", target_table, source_file)
        df = self.spark.read.parquet(source_file)
        missing_columns = [
            column for column, _ in TARGET_SCHEMA.values() if column not in df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Colunas finais de TARGET_SCHEMA ausentes no Parquet: {missing_columns}"
            )
        if not isinstance(df.schema[_ORIGIN].dataType, T.StringType):
            raise TypeError(f"Parquet: {_ORIGIN} deve ser string.")

        origins = df.select(_ORIGIN).distinct().persist()
        try:
            # Uma única ação resume as origens: None se não há linhas, 1 se
            # alguma origem for inválida e 0 caso contrário.
            origin = F.col(_ORIGIN)
            invalid = origin.isNull() | origin.rlike(r"(?U)^\s*$")
            status = origins.agg(F.max(F.when(invalid, 1).otherwise(0))).first()
            if status is None or status[0] is None:
                logger.info("Parquet vazio; nenhuma carga em %s.", target_table)
                return
            if status[0]:
                raise ValueError(f"Parquet: {_ORIGIN} não pode ser nulo ou vazio.")

            if not self.spark.catalog.tableExists(quoted_table):
                logger.info("Criando tabela Iceberg e inserindo em %s.", target_table)
                df.writeTo(quoted_table).using("iceberg").create()
                return

            target_schema = self.spark.table(quoted_table).schema
            if _ORIGIN not in target_schema.names:
                raise ValueError(f"Destino: coluna {_ORIGIN} ausente.")
            if not isinstance(target_schema[_ORIGIN].dataType, T.StringType):
                raise TypeError(f"Destino: {_ORIGIN} deve ser string.")

            # A view local (nome único por chamada) expõe as origens ao SQL do
            # Spark, evitando coletá-las no driver; é removida no finally.
            view = f"stg_load_origins_{uuid4().hex}"
            origins.createOrReplaceTempView(view)
            try:
                predicate = (
                    f"{_ORIGIN} IN (SELECT {_ORIGIN} FROM {_quote_identifier(view)})"
                )
                matches = not self.spark.sql(
                    f"SELECT 1 FROM {quoted_table} WHERE {predicate}"
                ).isEmpty()
                if matches:
                    if set(df.columns) != set(target_schema.names):
                        raise ValueError(
                            "Colunas do Parquet incompatíveis com o schema do destino."
                        )
                    # Falhas de tipos incompatíveis devem ocorrer antes do DELETE,
                    # para não apagar dados que a inserção não poderia repor.
                    # Apenas o schema é analisado; o DataFrame original é gravado.
                    _ = df.to(target_schema).schema
                    logger.info(
                        "Excluindo registros de origens reenviadas em %s.", target_table
                    )
                    self.spark.sql(f"DELETE FROM {quoted_table} WHERE {predicate}")

                logger.info("Inserindo registros em %s.", target_table)
                try:
                    df.writeTo(quoted_table).append()
                except Exception:
                    # Com DELETE já confirmado, o destino pode ficar sem as
                    # origens reenviadas até a entrada ser reprocessada.
                    if matches:
                        logger.error(
                            "Falha parcial: DELETE concluído em %s, mas a inserção "
                            "falhou. Verifique o destino e reprocesse a entrada completa.",
                            target_table,
                        )
                    raise
            finally:
                self.spark.catalog.dropTempView(view)
        finally:
            origins.unpersist()
