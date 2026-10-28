"""Coordenação sequencial da extração e transformação, sem regras de CDR.

Este módulo orquestra, para um lote de configurações, a execução de duas
etapas já implementadas em outros módulos: a extração (``extractors``), que
grava um Parquet intermediário, e a transformação (``transformers``), que lê
esse Parquet e grava o Parquet padronizado. Nenhuma regra de layout de CDR é
implementada aqui; o módulo apenas valida, resolve componentes, executa e
reporta o resultado de cada item.

Fluxo de ``run_etl``:
    1. Preparação de cada configuração (``_prepare_config``, em
       ``teleutils.core.orchestrators.config``); falha gera
       ``validation_error`` na etapa ``configuration``.
    2. Detecção de conflitos de caminhos entre itens (``_path_conflicts``);
       itens envolvidos geram ``validation_error`` na etapa ``paths``.
    3. Identificação do formato do schema (``_schema_format``); falha gera
       ``validation_error`` na etapa ``resolution``.
    4. Criação única, por lote, dos extratores e do transformador
       (``_create_components``); falha gera ``execution_error`` na etapa
       ``initialization`` para todos os itens ainda válidos.
    5. Resolução do extrator e do método ``transform_<schema>``
       (``_resolve_operations``), antes de qualquer escrita.
    6. Execução item a item (``_execute_item``): extração e, se concluída,
       transformação. A falha de um item não interrompe os demais.

Estruturas de dados:
    - ``ETLItemResult``: resultado imutável de um item.
    - ``ETLReport``: resultados na ordem original, com contagens derivadas.

Dependências:
    - ``teleutils.core.orchestrators.config``: validação e construção de
      caminhos.
    - ``teleutils.core.extractors`` e ``teleutils.core.transformers``:
      importados apenas durante a execução (imports adiados), de modo que a
      importação deste módulo não exige PySpark.
    - ``pyspark.sql.SparkSession``: sessão fornecida pelo chamador; este
      módulo não a cria nem a encerra.

Exemplo:
    >>> relatorio = run_etl([config_a, config_b], spark=spark)  # doctest: +SKIP
    >>> relatorio.successes, relatorio.execution_errors  # doctest: +SKIP
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol
from urllib.parse import unquote

from teleutils.core.orchestrators.config import (
    ConfigPaths,
    _prepare_config,
    _PreparedConfig,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_Status = Literal["success", "validation_error", "execution_error"]
_Step = Literal["extraction", "transformation"]
_Stage = Literal[
    "configuration",
    "paths",
    "resolution",
    "initialization",
    "extraction",
    "transformation",
    "completed",
]
_Format = Literal["parquet", "text"]


@dataclass(frozen=True)
class ETLItemResult:
    """Resultado imutável de um item; não certifica a integridade dos dados.

    Attributes:
        index: Posição baseada em zero na entrada do lote.
        schema: Chave informada, se identificável.
        paths: Caminhos planejados, ou None se a configuração não foi resolvida.
        status: success, validation_error ou execution_error.
        stage: Etapa da falha, ou completed quando ambas as escritas terminaram.
        completed_stages: Etapas concluídas, em ordem de execução.
        error_type: Nome da classe da exceção, ou None no sucesso.
        error_message: Mensagem original da falha, ou None no sucesso.
    """

    index: int
    schema: str | None
    paths: ConfigPaths | None
    status: _Status
    stage: _Stage
    completed_stages: tuple[_Step, ...] = ()
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ETLReport:
    """Relatório imutável com resultados ordenados e contagens derivadas.

    Attributes:
        items: Uma tupla de resultados, um para cada configuração recebida.
    """

    items: tuple[ETLItemResult, ...]

    @property
    def total(self) -> int:
        """Quantidade total de itens, incluindo configurações inválidas."""
        return len(self.items)

    @property
    def successes(self) -> int:
        """Quantidade de itens com extração e transformação concluídas."""
        return sum(item.status == "success" for item in self.items)

    @property
    def validation_errors(self) -> int:
        """Quantidade de falhas de configuração, caminhos ou resolução."""
        return sum(item.status == "validation_error" for item in self.items)

    @property
    def execution_errors(self) -> int:
        """Quantidade de falhas de inicialização, extração ou transformação."""
        return sum(item.status == "execution_error" for item in self.items)


class _Extractor(Protocol):
    """Contrato mínimo exigido dos extratores utilizados pelo orquestrador.

    Corresponde à interface de ``CDRParquetExtractor`` e ``CDRTextExtractor``:
    ``schemas`` expõe o catálogo de chaves aceitas e ``extract`` grava o
    Parquet intermediário em ``target_file``.
    """

    @property
    def schemas(self) -> Mapping[str, object]: ...

    def extract(
        self,
        source_file: str | list[str],
        target_file: str,
        cdr_schema: str,
        *,
        operator: str | None = "",
        service: str | None = "",
        cdr_type: str | None = "",
    ) -> str: ...


@dataclass(frozen=True)
class _Components:
    """Componentes compartilhados por todos os itens de um lote.

    Attributes:
        extractors: Extratores criados apenas para os formatos presentes no lote.
        transformer: Instância de ``CDRTransformer``; o método de cada item é
            obtido dinamicamente por nome (``transform_<schema>``).
    """

    extractors: tuple[_Extractor, ...]
    transformer: object


def _schema_format(schema: str) -> _Format:
    """Identifica se o schema pertence ao catálogo Parquet ou ao de texto.

    Args:
        schema: Chave exata do layout, comparada sem normalização.

    Returns:
        ``"parquet"`` ou ``"text"``, conforme o catálogo que contém a chave.

    Raises:
        ValueError: Se a chave estiver nos dois catálogos ou em nenhum deles.

    Notes:
        Os catálogos são importados dentro da função (import adiado).
    """
    from teleutils.core.extractors.schemas import (
        PARQUET_DEFAULT_SCHEMAS,
        TEXT_DEFAULT_SCHEMAS,
        resolve_cdr_schema,
    )

    parquet = schema in PARQUET_DEFAULT_SCHEMAS
    text = schema in TEXT_DEFAULT_SCHEMAS
    if parquet and text:
        raise ValueError(f"Schema '{schema}' é ambíguo entre os catálogos.")
    if parquet:
        resolve_cdr_schema(PARQUET_DEFAULT_SCHEMAS, schema)
        return "parquet"
    if text:
        resolve_cdr_schema(TEXT_DEFAULT_SCHEMAS, schema)
        return "text"
    raise ValueError(f"Schema '{schema}' não encontrado nos catálogos de extração.")


def _create_components(spark: SparkSession, formats: set[_Format]) -> _Components:
    """Instancia os extratores necessários e o transformador do lote.

    Args:
        spark: Sessão Spark do chamador, repassada aos componentes.
        formats: Formatos presentes nos itens válidos; apenas os extratores
            correspondentes são criados, cada um com uma cópia do catálogo
            padrão de schemas.

    Returns:
        Componentes compartilhados por todos os itens do lote.

    Notes:
        Imports adiados: ``extractors`` e ``transformers`` só são importados
        quando há ao menos um item válido a executar.
    """
    from teleutils.core.extractors import CDRParquetExtractor, CDRTextExtractor
    from teleutils.core.extractors.schemas import (
        PARQUET_DEFAULT_SCHEMAS,
        TEXT_DEFAULT_SCHEMAS,
    )
    from teleutils.core.transformers import CDRTransformer

    extractors: list[_Extractor] = []
    if "parquet" in formats:
        extractors.append(CDRParquetExtractor(spark, dict(PARQUET_DEFAULT_SCHEMAS)))
    if "text" in formats:
        extractors.append(CDRTextExtractor(spark, dict(TEXT_DEFAULT_SCHEMAS)))
    return _Components(tuple(extractors), CDRTransformer(spark))


def _resolve_operations(
    components: _Components, schema: str
) -> tuple[_Extractor, Callable[[str, str], object]]:
    """Resolve o extrator e o método de transformação de um schema.

    Args:
        components: Componentes criados para o lote.
        schema: Chave exata do layout.

    Returns:
        Tupla com o extrator cujo catálogo contém o schema e o método
        ``transform_<schema>`` do transformador, chamado depois como
        ``transform(extracted_file, transformed_file)``.

    Raises:
        ValueError: Se o schema não pertencer a exatamente um extrator.
        TypeError: Se o transformador não tiver método chamável
            ``transform_<schema>``.
    """
    from teleutils.core.extractors.schemas import resolve_cdr_schema

    matches = [
        extractor for extractor in components.extractors if schema in extractor.schemas
    ]
    if len(matches) != 1:
        raise ValueError(f"Schema '{schema}' deve pertencer a exatamente um extrator.")
    extractor = matches[0]
    resolve_cdr_schema(extractor.schemas, schema)
    method_name = f"transform_{schema}"
    method = getattr(components.transformer, method_name, None)
    if not callable(method):
        raise TypeError(
            f"Transformador '{method_name}' não encontrado ou não chamável."
        )
    return extractor, method


def _overlap(left: str, right: str) -> bool:
    """Indica se dois URIs se sobrepõem na hierarquia de diretórios.

    Dois URIs se sobrepõem quando possuem o mesmo esquema e a mesma autoridade
    (bucket, no caso de ``s3a``) e o caminho de um é igual ou ancestral do
    outro, comparados por segmentos e ignorando segmentos vazios. Esquemas ou
    autoridades diferentes nunca se sobrepõem.

    Args:
        left: URI no formato ``esquema://autoridade/caminho``.
        right: URI no mesmo formato.

    Returns:
        ``True`` se houver sobreposição; ``False`` caso contrário.
    """
    left_scheme, _, left_location = left.partition("://")
    right_scheme, _, right_location = right.partition("://")
    if left_scheme != right_scheme:
        return False
    left_authority, _, left_path = left_location.partition("/")
    right_authority, _, right_path = right_location.partition("/")
    if left_authority != right_authority:
        return False
    # S3A mantém a comparação conservadora existente; nomes locais são literais.
    if left_scheme == "s3a":
        left_path, right_path = unquote(left_path), unquote(right_path)
    left_parts = tuple(part for part in left_path.split("/") if part)
    right_parts = tuple(part for part in right_path.split("/") if part)
    size = min(len(left_parts), len(right_parts))
    return left_parts[:size] == right_parts[:size]


def _path_conflicts(prepared: Mapping[int, _PreparedConfig]) -> dict[int, str]:
    """Detecta conflitos de caminhos entre os itens já preparados do lote.

    Dois tipos de conflito são verificados com ``_overlap``:

    * entre os destinos (``extracted_file`` e ``transformed_file``) de itens
      diferentes ou do mesmo item (por exemplo, quando um destino contém o
      outro);
    * entre cada destino e a ``source_folder`` de qualquer item, inclusive o
      próprio.

    Args:
        prepared: Configurações preparadas, indexadas pela posição original.

    Returns:
        Mapeamento do índice de cada item envolvido em conflito para a
        mensagem do último conflito registrado para ele. Ambos os itens de um
        par conflitante são incluídos.
    """
    outputs = [
        (index, path)
        for index, config in prepared.items()
        for path in (config.paths.extracted_file, config.paths.transformed_file)
    ]
    conflicts: dict[int, str] = {}
    for position, (index, path) in enumerate(outputs):
        for other, other_path in outputs[position + 1 :]:
            if _overlap(path, other_path):
                message = f"Destinos conflitantes entre os itens {index} e {other}."
                conflicts[index] = conflicts[other] = message
        for other, config in prepared.items():
            if _overlap(path, config.source_folder):
                message = f"Destino do item {index} sobrepõe a origem do item {other}."
                conflicts[index] = conflicts[other] = message
    return conflicts


def _failure(
    index: int,
    schema: str | None,
    paths: ConfigPaths | None,
    status: _Status,
    stage: _Stage,
    error: Exception,
    completed: tuple[_Step, ...] = (),
) -> ETLItemResult:
    """Constrói o ``ETLItemResult`` de uma falha a partir da exceção capturada.

    ``error_type`` recebe o nome da classe da exceção e ``error_message``,
    a sua mensagem original.
    """
    return ETLItemResult(
        index,
        schema,
        paths,
        status,
        stage,
        completed,
        type(error).__name__,
        str(error),
    )


def _execute_item(
    index: int,
    config: _PreparedConfig,
    extractor: _Extractor,
    transform: Callable[[str, str], object],
) -> ETLItemResult:
    """Executa a extração e a transformação de um item já resolvido.

    A transformação só é iniciada se a extração terminar sem exceção. Qualquer
    ``Exception`` é registrada em log e convertida em ``execution_error`` na
    etapa em que ocorreu, preservando em ``completed_stages`` as etapas
    concluídas antes da falha. Intermediários e saídas parciais não são
    removidos.

    Args:
        index: Posição do item na entrada do lote.
        config: Configuração preparada do item.
        extractor: Extrator resolvido para o schema do item.
        transform: Método ``transform_<schema>`` resolvido, chamado com os
            caminhos do Parquet extraído e do Parquet transformado.

    Returns:
        Resultado de sucesso (etapa ``completed``) ou de ``execution_error``.
    """
    paths = config.paths
    stage: _Step = "extraction"
    completed: tuple[_Step, ...] = ()
    logger.info("Iniciando item %d, schema=%r.", index, paths.schema)
    try:
        extractor.extract(
            paths.source_file,
            paths.extracted_file,
            paths.schema,
            operator=config.operator,
            service=config.service,
            cdr_type=config.cdr_type,
        )
        completed = ("extraction",)
        stage = "transformation"
        transform(paths.extracted_file, paths.transformed_file)
    except Exception as error:
        logger.exception(
            "Falha no item %d, schema=%r, etapa=%s.", index, paths.schema, stage
        )
        return _failure(
            index, paths.schema, paths, "execution_error", stage, error, completed
        )
    logger.info("Item %d concluído, schema=%r.", index, paths.schema)
    return ETLItemResult(
        index,
        paths.schema,
        paths,
        "success",
        "completed",
        ("extraction", "transformation"),
    )


def run_etl(
    configs: Sequence[Mapping[str, object]],
    *,
    spark: SparkSession,
    base_folder: str | None = None,
    week_dir: str | None = None,
) -> ETLReport:
    """Executa extração e transformação sequenciais, continuando após falhas.

    Args:
        configs: Sequência finita de configurações aceitas por get_config.
            Todos os caminhos são preparados antes das escritas. Itens
            envolvidos em conflitos são rejeitados, inclusive entre itens.
            Uma sequência vazia retorna relatório vazio sem criar componentes.
        spark: Sessão existente. Componentes são criados uma vez por lote;
            o contexto Spark do chamador não é criado nem encerrado.
        base_folder: Pasta base opcional a ser prefixada à origem configurada.
            Deve ser informada junto com ``week_dir``.
        week_dir: Pasta da semana opcional, concatenada entre ``base_folder``
            e a ``source_folder`` da configuração.

    Returns:
        Relatório com um resultado por item na ordem original. Falhas de
        configuração, resolução e execução são registradas e retornadas.
        ValueError durante processamento é execution_error, não erro de
        configuração. Não há carga, retry, rollback ou certificação de dados.

    Raises:
        TypeError: Se configs não for sequência ou for string/mapping.
        ValueError: Se apenas um entre ``base_folder`` e ``week_dir`` for
            informado ou se um deles for vazio.
        KeyboardInterrupt: Interrupções do usuário não são capturadas.

    Notes:
        O método de transformação é resolvido antes da extração, inclusive
        para aliases. Não há regras específicas por layout. Falhas na
        inicialização compartilhada impedem todos os itens ainda válidos.
        Os componentes mantêm sua semântica de overwrite e tolerância a
        dados ausentes/corrompidos. Intermediários e saídas parciais não
        são removidos em caso de falha.
    """
    if not isinstance(configs, Sequence) or isinstance(configs, (str, bytes, Mapping)):
        logger.error("configs deve ser uma sequência de configurações.")
        raise TypeError("configs deve ser uma sequência de configurações.")
    if (base_folder is None) != (week_dir is None):
        logger.error("base_folder e week_dir devem ser informados em conjunto.")
        raise ValueError("base_folder e week_dir devem ser informados em conjunto.")
    if base_folder is not None and week_dir is not None:
        for name, folder in (("base_folder", base_folder), ("week_dir", week_dir)):
            if not isinstance(folder, str):
                logger.error("%s deve ser uma string.", name)
                raise TypeError(f"{name} deve ser uma string.")
            if not folder.strip():
                logger.error("%s não pode ser vazio.", name)
                raise ValueError(f"{name} não pode ser vazio.")
    results: dict[int, ETLItemResult] = {}
    prepared: dict[int, _PreparedConfig] = {}
    # Etapa 1: configurações inválidas são registradas e não participam das demais.
    for index, raw_config in enumerate(configs):
        try:
            item_config = raw_config
            if (
                base_folder is not None
                and week_dir is not None
                and isinstance(raw_config, Mapping)
            ):
                source_folder = raw_config.get("source_folder")
                if isinstance(source_folder, str):
                    item_config = dict(raw_config)
                    item_config["source_folder"] = (
                        f"{base_folder.rstrip('/')}/{week_dir.strip('/')}/"
                        f"{source_folder.lstrip('/')}"
                    )
            prepared[index] = _prepare_config(item_config)
        except (TypeError, ValueError) as error:
            schema = (
                raw_config.get("schema") if isinstance(raw_config, Mapping) else None
            )
            schema = schema if isinstance(schema, str) else None
            logger.warning("Configuração inválida no item %d: %s", index, error)
            results[index] = _failure(
                index, schema, None, "validation_error", "configuration", error
            )
    # Etapa 2: ambos os itens de um par conflitante são rejeitados, pois a
    # sobreposição é verificada antes de qualquer escrita.
    for index, message in _path_conflicts(prepared).items():
        paths = prepared.pop(index).paths
        logger.warning("Conflito de caminhos no item %d: %s", index, message)
        results[index] = _failure(
            index, paths.schema, paths, "validation_error", "paths", ValueError(message)
        )
    # Etapa 3: os formatos coletados definem quais extratores serão criados.
    formats: set[_Format] = set()
    for index, config in tuple(prepared.items()):
        try:
            formats.add(_schema_format(config.paths.schema))
        except (TypeError, ValueError) as error:
            logger.warning("Falha de resolução no item %d: %s", index, error)
            results[index] = _failure(
                index,
                config.paths.schema,
                config.paths,
                "validation_error",
                "resolution",
                error,
            )
            del prepared[index]
    if prepared:
        try:
            components = _create_components(spark, formats)
        except Exception as error:
            logger.exception("Falha na inicialização dos componentes ETL.")
            for index, config in prepared.items():
                results[index] = _failure(
                    index,
                    config.paths.schema,
                    config.paths,
                    "execution_error",
                    "initialization",
                    error,
                )
        else:
            # Todas as operações são resolvidas antes da primeira execução;
            # assim, uma falha de resolução não ocorre após escritas já feitas.
            operations: dict[int, tuple[_Extractor, Callable[[str, str], object]]] = {}
            for index, config in prepared.items():
                try:
                    operations[index] = _resolve_operations(
                        components, config.paths.schema
                    )
                except (TypeError, ValueError) as error:
                    logger.warning("Falha de resolução no item %d: %s", index, error)
                    results[index] = _failure(
                        index,
                        config.paths.schema,
                        config.paths,
                        "validation_error",
                        "resolution",
                        error,
                    )
            # Execução sequencial na ordem de inserção dos itens resolvidos.
            for index, (extractor, transform) in operations.items():
                results[index] = _execute_item(
                    index, prepared[index], extractor, transform
                )
    report = ETLReport(tuple(results[index] for index in range(len(configs))))
    logger.info(
        "Lote encerrado: total=%d, sucessos=%d, falhas_validacao=%d, falhas_execucao=%d.",
        report.total,
        report.successes,
        report.validation_errors,
        report.execution_errors,
    )
    return report


def run_etl_item(config: Mapping[str, object], *, spark: SparkSession) -> ETLItemResult:
    """Executa um único item usando o mesmo contrato e tratamento do lote.

    Args:
        config: Configuração de extração e transformação aceita por get_config.
        spark: Sessão Spark existente, não encerrada pelo orquestrador.

    Returns:
        Resultado com index=0. Falhas do item são registradas e retornadas,
        sem converter operações incompletas em sucesso.
    """
    return run_etl((config,), spark=spark).items[0]
