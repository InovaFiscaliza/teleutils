"""Carga de Parquet em tabelas Iceberg compartilhadas com o Trino.

A escrita ocorre pelo Spark, não por uma conexão ao coordenador Trino. A sessão
deve ter as extensões Iceberg e acesso ao catálogo utilizado pelo destino.

Responsabilidades:
    - Ler o Parquet padronizado produzido pela etapa de transformação.
    - Verificar as colunas finais obrigatórias e, quando utilizadas, as chaves.
    - Criar a tabela ou carregar registros por MERGE INTO ou append.
    - Validar e registrar origens em controle Parquet ou Iceberg.

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
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import PRIMARY_KEY_COLUMNS, TARGET_SCHEMA
from teleutils._logging import log_operation

logger = logging.getLogger(__name__)

_CONTROL_SCHEMA = T.StructType(
    [
        T.StructField("no_arquivo_origem", T.StringType(), False),
        T.StructField("dh_carga", T.TimestampNTZType(), False),
    ]
)


def _quote_identifier(identifier: str) -> str:
    """Delimita um identificador Spark SQL e escapa suas crases internas.

    O argumento representa um único nome literal, não um caminho qualificado.
    Pontos no argumento permanecem parte do identificador delimitado.

    Args:
        identifier: Nome literal de um único componente (catálogo, schema ou
            tabela, ou nome da view temporária).

    Returns:
        O nome entre crases, com cada crase interna duplicada.
    """
    return "`" + identifier.replace("`", "``") + "`"


def _table_parts(name: str, argument: str) -> list[str]:
    if not isinstance(name, str):
        raise TypeError(f"{argument} deve ser um nome de tabela.")
    parts = name.split(".")
    if not 1 <= len(parts) <= 3 or any(not part.strip() for part in parts):
        raise ValueError(f"{argument} deve identificar uma tabela válida.")
    return parts


def _validate_origins(df: DataFrame, label: str) -> None:
    if not isinstance(df.schema["no_arquivo_origem"].dataType, T.StringType):
        raise TypeError(f"{label}: no_arquivo_origem deve ser string.")
    name = F.col("no_arquivo_origem")
    if not df.filter(name.isNull() | name.rlike(r"(?U)^\s*$")).isEmpty():
        raise ValueError(f"{label}: no_arquivo_origem não pode ser nulo ou vazio.")


def _validate_control(df: DataFrame) -> None:
    if len(df.columns) != 2 or set(df.columns) != set(_CONTROL_SCHEMA.names):
        raise ValueError(
            "control_table deve conter apenas no_arquivo_origem e dh_carga."
        )
    if not isinstance(df.schema["dh_carga"].dataType, T.TimestampNTZType):
        raise TypeError("control_table: dh_carga deve ser timestamp sem fuso.")
    _validate_origins(df, "control_table")
    if not df.filter(F.col("dh_carga").isNull()).isEmpty():
        raise ValueError("control_table: dh_carga não pode ser nulo.")
    if not df.groupBy("no_arquivo_origem").count().filter("count > 1").isEmpty():
        raise ValueError("control_table deve ter uma única linha por origem.")


def _is_control_path(control_table: str) -> bool:
    return control_table.startswith(("s3a://", "file://"))


def _validate_control_uri(value: str) -> None:
    uri = urlsplit(value)
    decoded_path = unquote(uri.path)
    if (
        uri.query
        or uri.fragment
        or not decoded_path.startswith("/")
        or not decoded_path.strip("/")
        or any(part in (".", "..") for part in decoded_path.split("/"))
        or any(char in decoded_path for char in "*?[]{}\\")
        or any(ord(char) < 32 for char in value + decoded_path)
        or value != value.strip()
    ):
        raise ValueError(
            "control_table deve identificar um diretório seguro, não uma raiz."
        )
    if uri.scheme == "s3a" and (
        not uri.netloc or "@" in uri.netloc or ":" in uri.netloc
    ):
        raise ValueError("control_table s3a:// requer bucket e prefixo válidos.")
    if uri.scheme == "file" and uri.netloc:
        raise ValueError("Use file:///caminho absoluto, sem hostname.")


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
        *,
        mode: Literal["merge", "insert_only", "append"] = "merge",
        control_table: str | None = None,
    ) -> None:
        """Lê Parquet e carrega registros conforme o modo de escrita.

        Fluxo de processamento:
            1. Valida o modo, o destino e, fora de append, os nomes da chave.
            2. Se informado, cria/valida o controle e testa sua gravação.
            3. Lê o Parquet e verifica as colunas finais obrigatórias.
            4. Fora de append, verifica as chaves e deduplica a origem por elas.
            5. Cria a tabela Iceberg ausente ou escreve na existente usando
               MERGE por uma view temporária ou append direto.
            Com controle habilitado, filtra origens já carregadas antes da
            deduplicação (exceto em merge) e registra sucesso após a escrita.

        Args:
            source_file: Caminho do arquivo ou diretório Parquet padronizado.
            target_table: Nome do destino, preferencialmente catalogo.schema.tabela,
                com componentes sem delimitadores SQL. Aceita de um a três
                componentes separados por pontos, nenhum deles vazio ou composto
                apenas por espaços. Cada componente é delimitado em crases.
            primary_keys: Sequência não vazia de nomes de colunas existentes,
                sem repetições nem nomes vazios ou compostos apenas por espaços.
                Usa PRIMARY_KEY_COLUMNS por padrão e copia a sequência antes
                de utilizá-la, sem modificar o objeto recebido. Ignorada em append.
            mode: merge atualiza registros correspondentes e insere novos
                (padrão); insert_only insere somente chaves ausentes no destino;
                append insere todos os registros, inclusive duplicados, sem
                validar, deduplicar ou comparar chaves.
            control_table: URI de dataset Parquet iniciada por s3a:// ou file://;
                outras strings identificam tabela Iceberg no catálogo Spark
                compartilhado com Trino. None desabilita o controle. Não aceita
                DataFrame. O controle contém apenas no_arquivo_origem (string)
                e dh_carga (TimestampNTZType). Não pode sobrepor origem/destino.

        Returns:
            None: Os dados são persistidos na tabela; não retorna DataFrame
            nem contagem de registros inseridos ou atualizados.

        Raises:
            ValueError: Se o modo ou destino forem inválidos, faltarem colunas
                finais de TARGET_SCHEMA ou, fora de append, a chave for inválida
                ou suas colunas estiverem ausentes no Parquet.
                Também se a configuração, o schema ou o conteúdo do controle
                forem inválidos, ou houver origens nulas/vazias com controle ativo.
            TypeError: Se os tipos das opções ou das colunas de controle forem
                inválidos.

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

            Em merge e insert_only, a origem é deduplicada pela chave; se registros
            da mesma chave divergirem, não há garantia de qual será mantido.
            Chaves nulas são comparadas com igualdade null-safe no MERGE.
            A correspondência exige igualdade em todas as colunas da chave.
            Correspondentes são atualizados apenas em merge; os demais são
            inseridos em ambos os modos. Em append, todas as linhas são mantidas,
            inclusive na criação da tabela, sem consulta às chaves do destino.
            Não há remoção de registros do destino nem criação de restrição de
            chave primária. A deduplicação afeta apenas a origem desta chamada.
            UPDATE SET * e INSERT * pressupõem schemas compatíveis entre origem
            e destino. Não são reaplicadas as transformações de CDR.

            dropDuplicates constrói um plano lazy; create(), append() ou MERGE
            materializam o processamento distribuído e a persistência.
            O caminho de MERGE registra uma view local com sufixo UUID na sessão
            recebida e tenta removê-la no finally, inclusive se o SQL falhar.
            A remoção não possui tratamento próprio de exceções.

            log_operation registra início, sucesso ou falha e relança exceções.
            Não há retry nem recuperação de falhas de leitura, catálogo ou carga.
            A configuração dos handlers de logging cabe à aplicação consumidora.

            O controle mantém uma linha por origem e atualiza dh_carga em recargas
            merge. O valor é UTC por convenção, armazenado sem fuso. Nomes são
            comparados exatamente; arquivos físicos não definem a identidade.
            Em append e insert_only, origens presentes no controle são puladas
            com log. Sem origens elegíveis, não há carga de dados; o controle
            vazio e os metadados das sondagens podem permanecer.
            Origens consumidas são registradas mesmo quando suas linhas forem
            deduplicadas ou insert_only não inserir nenhuma chave nova.

            Controle ausente é criado vazio antes da leitura da origem. Falhas
            na criação, leitura, sondagem de escrita ou cleanup impedem a carga.
            A sondagem em tabela usa append/MERGE vazios e pode criar snapshots,
            sem alterar registros. Em Parquet, grava e remove datasets temporários.
            Catálogo e namespace devem existir. Cada controle pertence a um
            único destino e chamadas devem ser serializadas. Dados e controle
            não têm transação conjunta: falha no registro após a carga é
            propagada com log de falha parcial; repetir append pode duplicar
            dados. O controle Parquet é sobrescrito após materialização em
            temporário independente, sem backup ou rollback: uma falha pode
            perder o controle anterior. Sondagens não garantem commits futuros.
            A origem deve permanecer estável e leitores externos não devem
            acessar o controle Parquet durante a sobrescrita.

            Anotação de manutenção: alterações nos nomes finais de TARGET_SCHEMA
            impactam a validação de todo Parquet, mesmo com chaves personalizadas.
            Alterações em PRIMARY_KEY_COLUMNS impactam a deduplicação e o ON
            quando primary_keys não é fornecido, exceto em append.
        """
        if mode not in ("merge", "insert_only", "append"):
            raise ValueError("mode deve ser 'merge', 'insert_only' ou 'append'.")

        keys = list(primary_keys) if mode != "append" else []
        if mode != "append":
            if not keys or any(
                not isinstance(key, str) or not key.strip() for key in keys
            ):
                raise ValueError(
                    "primary_keys deve conter nomes de colunas não vazios."
                )
            if len(keys) != len(set(keys)):
                raise ValueError("primary_keys não deve conter colunas repetidas.")

        table_parts = _table_parts(target_table, "target_table")
        quoted_table = ".".join(_quote_identifier(part) for part in table_parts)
        resolved_control = self._resolve_control(table_parts, control_table)
        control = (
            self._prepare_control(resolved_control, source_file, quoted_table)
            if resolved_control is not None
            else None
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
        missing_keys = [key for key in keys if key not in df.columns]
        if missing_keys:
            raise ValueError(f"Colunas da chave ausentes no Parquet: {missing_keys}")

        if resolved_control is not None and control is not None:
            self._load_with_control(
                df, quoted_table, keys, mode, resolved_control, control
            )
        else:
            self._write_data(df, quoted_table, keys, mode)

    def _resolve_control(
        self,
        target_parts: list[str],
        control_table: str | None,
    ) -> str | None:
        if control_table is None:
            return None
        if not isinstance(control_table, str):
            raise TypeError("control_table deve ser string ou None.")
        if _is_control_path(control_table):
            _validate_control_uri(control_table)
            uri = urlsplit(control_table)
            return f"{uri.scheme}://{uri.netloc}{unquote(uri.path).rstrip('/')}"
        if any(char in control_table for char in "/\\:"):
            raise ValueError(
                "Para caminhos de controle, use s3a:// ou file:///; caso contrário, informe uma tabela."
            )
        parts = _table_parts(control_table, "control_table")
        catalog = self.spark.catalog.currentCatalog()
        namespace = self.spark.catalog.currentDatabase()

        def qualified(components: list[str]) -> list[str]:
            if len(components) == 1:
                components = [catalog, namespace, *components]
            elif len(components) == 2:
                components = [catalog, *components]
            return [component.casefold() for component in components]

        if qualified(parts) == qualified(target_parts):
            raise ValueError("A tabela de controle deve ser diferente de target_table.")
        return ".".join(_quote_identifier(part) for part in parts)

    def _hadoop_path(self, value: str) -> tuple[Any, Any]:
        context = self.spark.sparkContext
        # Py4J resolve classes e métodos Hadoop dinamicamente no gateway JVM.
        jvm, java_context = cast(Any, context._jvm), cast(Any, context._jsc)
        if jvm is None or java_context is None:
            raise RuntimeError("A sessão Spark não possui um contexto Hadoop ativo.")
        path = jvm.org.apache.hadoop.fs.Path(value)
        return path, path.getFileSystem(java_context.hadoopConfiguration())

    def _path_identity(self, value: str) -> tuple[str, str, str]:
        path, filesystem = self._hadoop_path(value)
        uri = filesystem.makeQualified(path).toUri().normalize()
        scheme = str(uri.getScheme()).lower()
        authority = str(uri.getAuthority() or "").lower()
        path_string = str(uri.getPath())
        if scheme == "file":
            path_string = str(Path(path_string).resolve())
        return scheme, authority, path_string.rstrip("/") or "/"

    def _validate_path_separation(
        self, control_table: str, source_file: str, target_table: str
    ) -> None:
        control_identity = self._path_identity(control_table)
        if control_identity[2] == "/":
            raise ValueError(
                "control_table não pode resolver para a raiz do filesystem."
            )
        locations = [source_file]
        if self.spark.catalog.tableExists(target_table):
            details = self.spark.sql(
                f"DESCRIBE TABLE EXTENDED {target_table}"
            ).collect()
            locations.extend(
                row.data_type for row in details if row.col_name == "Location"
            )
        for location in locations:
            scheme, authority, path = self._path_identity(location)
            # Globs da origem representam toda a árvore a partir do primeiro padrão.
            components = path.split("/")
            for index, part in enumerate(components):
                if any(char in part for char in "*?[]{}"):
                    path = "/".join(components[:index]) or "/"
                    break
            if (scheme, authority) != control_identity[:2]:
                continue
            control_path = control_identity[2]
            if (
                path == control_path
                or path.startswith(control_path.rstrip("/") + "/")
                or control_path.startswith(path.rstrip("/") + "/")
            ):
                raise ValueError(
                    "control_table não pode sobrepor a origem ou o destino dos dados."
                )

    @contextmanager
    def _temporary_control_path(
        self, control_table: str, *, inside: bool = False
    ) -> Generator[str, None, None]:
        suffix = uuid4().hex
        temporary = (
            f"{control_table.rstrip('/')}/_teleutils_probe_{suffix}"
            if inside
            else f"{control_table.rstrip('/')}.teleutils_{suffix}"
        )
        path, filesystem = self._hadoop_path(temporary)
        if filesystem.exists(path):
            raise FileExistsError(f"Temporário de controle já existe: {temporary}")
        failed = False
        try:
            yield temporary
        except Exception:
            failed = True
            raise
        finally:
            try:
                if filesystem.exists(path) and not filesystem.delete(path, True):
                    raise OSError(
                        f"Não foi possível remover temporário de controle: {temporary}"
                    )
            except Exception:
                logger.exception(
                    "Falha na limpeza do temporário de controle %s.", temporary
                )
                if not failed:
                    raise

    def _prepare_control(
        self, control_table: str, source_file: str, target_table: str
    ) -> DataFrame:
        logger.info(
            "Validando leitura e gravação do controle %s antes da carga.", control_table
        )
        empty = self.spark.createDataFrame([], _CONTROL_SCHEMA)
        if _is_control_path(control_table):
            self._validate_path_separation(control_table, source_file, target_table)
            path, filesystem = self._hadoop_path(control_table)
            if not filesystem.exists(path):
                logger.info(
                    "Controle %s não existe; criando Parquet vazio antes da carga.",
                    control_table,
                )
                empty.write.mode("errorifexists").parquet(control_table)
            elif not filesystem.isDirectory(path):
                raise ValueError(
                    "control_table deve apontar para um diretório Parquet."
                )
            control = self.spark.read.parquet(control_table)
            _validate_control(control)
            for inside in (True, False):
                with self._temporary_control_path(
                    control_table, inside=inside
                ) as probe:
                    empty.write.mode("errorifexists").parquet(probe)
        else:
            if not self.spark.catalog.tableExists(control_table):
                logger.info(
                    "Controle %s não existe; criando tabela vazia antes da carga.",
                    control_table,
                )
                empty.writeTo(control_table).using("iceberg").create()
            self.spark.catalog.refreshTable(control_table)
            control = self.spark.table(control_table)
            _validate_control(control)
            empty.writeTo(control_table).append()
            self._merge_control(empty, control_table)
        return control

    def _load_with_control(
        self,
        df: DataFrame,
        target_table: str,
        keys: list[str],
        mode: Literal["merge", "insert_only", "append"],
        control_table: str,
        control: DataFrame,
    ) -> None:
        _validate_origins(df, "Parquet")
        origins = df.select("no_arquivo_origem").distinct()
        known = control.select("no_arquivo_origem")
        for row in origins.join(
            known, "no_arquivo_origem", "left_semi"
        ).toLocalIterator():
            if mode == "merge":
                logger.info(
                    "Recarregando origem já carregada: %r", row.no_arquivo_origem
                )
            else:
                logger.info("Pulando origem já carregada: %r", row.no_arquivo_origem)
        if mode != "merge":
            origins = origins.join(known, "no_arquivo_origem", "left_anti")

        # Materializa os nomes antes de deduplicar dados ou modificar o controle.
        origins = origins.persist()
        try:
            if origins.count() == 0:
                logger.info("Nenhuma origem elegível para carga em %s.", target_table)
                return
            eligible = df.join(origins, "no_arquivo_origem", "left_semi")
            self._write_data(eligible, target_table, keys, mode)
            try:
                self._record_control(origins, control_table, control)
            except Exception:
                logger.exception(
                    "Falha parcial: dados gravados em %s, mas registro no controle %s "
                    "ou sua limpeza falhou. Repetir append pode duplicar dados; "
                    "overwrite Parquet pode perder o controle anterior.",
                    target_table,
                    control_table,
                )
                raise
        finally:
            origins.unpersist()

    def _record_control(
        self,
        origins: DataFrame,
        control_table: str,
        control: DataFrame,
    ) -> None:
        loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
        timestamp = self.spark.createDataFrame(
            [(loaded_at,)], T.StructType([_CONTROL_SCHEMA["dh_carga"]])
        )
        updates = origins.crossJoin(timestamp)
        if _is_control_path(control_table):
            unchanged = control.join(origins, "no_arquivo_origem", "left_anti")
            with self._temporary_control_path(control_table) as temporary:
                unchanged.unionByName(updates).select(
                    *_CONTROL_SCHEMA.names
                ).write.mode("errorifexists").parquet(temporary)
                # O overwrite não pode depender dos arquivos que ele próprio remove.
                self.spark.read.parquet(temporary).write.mode("overwrite").parquet(
                    control_table
                )
                logger.info(
                    "Controle Parquet gravado em %s; removendo temporário.",
                    control_table,
                )
        else:
            self._merge_control(updates, control_table)
        logger.info("Controle de carga registrado em %s.", control_table)

    def _merge_control(self, updates: DataFrame, control_table: str) -> None:
        view = f"stg_load_control_{uuid4().hex}"
        updates.createOrReplaceTempView(view)
        try:
            self.spark.sql(
                f"""
                    MERGE INTO {control_table} AS target
                    USING {_quote_identifier(view)} AS source
                    ON target.no_arquivo_origem = source.no_arquivo_origem
                    WHEN MATCHED THEN UPDATE SET dh_carga = source.dh_carga
                    WHEN NOT MATCHED THEN INSERT (no_arquivo_origem, dh_carga)
                    VALUES (source.no_arquivo_origem, source.dh_carga)
                """
            )
        finally:
            self.spark.catalog.dropTempView(view)

    def _write_data(
        self,
        df: DataFrame,
        target_table: str,
        keys: list[str],
        mode: Literal["merge", "insert_only", "append"],
    ) -> None:
        if mode != "append":
            df = df.dropDuplicates(keys)
        if not self.spark.catalog.tableExists(target_table):
            logger.info(
                "Criando tabela Iceberg e inserindo registros: %s", target_table
            )
            df.writeTo(target_table).using("iceberg").create()
            return

        if mode == "append":
            logger.info("Acrescentando todos os registros em %s", target_table)
            df.writeTo(target_table).append()
            return

        temp_view_name = f"stg_source_updates_{uuid4().hex}"
        join_condition = " AND ".join(
            f"target.{_quote_identifier(key)} <=> source.{_quote_identifier(key)}"
            for key in keys
        )
        df.createOrReplaceTempView(temp_view_name)
        try:
            if mode == "merge":
                logger.info("Atualizando e inserindo registros em %s", target_table)
            else:
                logger.info(
                    "Inserindo apenas registros com chaves novas em %s", target_table
                )
            matched_clause = "WHEN MATCHED THEN UPDATE SET *" if mode == "merge" else ""
            self.spark.sql(
                f"""
                MERGE INTO {target_table} AS target
                USING {_quote_identifier(temp_view_name)} AS source
                ON {join_condition}
                {matched_clause}
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            self.spark.catalog.dropTempView(temp_view_name)

    def append_to_iceberg(
        self,
        source_file: str,
        target_table: str,
        *,
        control_table: str | None = None,
    ) -> None:
        """Delega a carga para upsert_iceberg com mode="append".

        Args:
            source_file: Caminho do arquivo ou diretório Parquet padronizado.
            target_table: Nome da tabela Iceberg de destino.
            control_table: URI Parquet ou nome de tabela, conforme upsert_iceberg.

        Notes:
            Preserva todos os registros, inclusive duplicados, sem comparar
            chaves. Com controle ativo, pula origens já carregadas. Reutiliza
            as validações, a criação da tabela ausente e os logs de upsert_iceberg,
            propagando suas exceções.
        """
        if control_table is None:
            self.upsert_iceberg(source_file, target_table, mode="append")
        else:
            self.upsert_iceberg(
                source_file,
                target_table,
                mode="append",
                control_table=control_table,
            )
