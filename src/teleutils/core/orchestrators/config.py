"""Configurações ETL, construção de caminhos e listagem de arquivos no MinIO.

Valida um item de configuração (mapping) e deriva os caminhos Spark/Hadoop da
origem, do Parquet extraído e do Parquet transformado. ``load_config`` lê um
arquivo JSON local ou S3A; ``list_files`` lista arquivos e datasets no MinIO.
A construção de caminhos não acessa armazenamento.

Fluxo de ``_prepare_config``:
    mapping de entrada
      ↓ tipo, chaves desconhecidas/ausentes e ``storage_type``
      ↓ campos obrigatórios (strings não vazias) e metadados
      ↓ pastas (``s3a://`` ou ``file://``), ``pattern`` e ``output_file_prefix``
      ↓ ``ConfigPaths`` + ``_PreparedConfig``

Principais componentes:
    - ``load_config``: lê uma lista JSON e verifica a unicidade dos prefixos.
    - ``list_files``: lista arquivos, tratando diretórios Parquet como datasets.
    - ``get_config``: API pública; retorna somente ``ConfigPaths``.
    - ``_validate_unique_prefixes``: verifica prefixos repetidos no lote.
    - ``_prepare_config``: validação completa. Além dos caminhos, preserva a
      ``source_folder`` normalizada e os metadados, usados pelo orquestrador
      (``etl.py``) para detectar sobreposição entre origem e destinos e para
      chamar os extratores.

Dependências:
    - ``teleutils.core.extractors._extractors_utils.validate_metadata_arguments``,
      importada dentro de ``_prepare_config`` para não exigir sessão Spark.

Dicionário de configuração:
    Cada item é um mapping (``dict``) com as chaves abaixo. Qualquer outra
    chave é rejeitada com ``ValueError``. Valores obrigatórios devem ser
    ``str`` não vazia e sem somente espaços (``TypeError`` para outro tipo).
    O mapping de entrada não é modificado.

    Chaves obrigatórias:
        source_folder (str): Pasta de origem dos arquivos de CDR, sem o
            ``pattern``. Interpretada conforme ``storage_type`` (ver abaixo).
        pattern (str): Padrão de arquivos relativo à ``source_folder``, com
            curingas ``*?[]{}`` permitidos (ex.: ``"*.parquet"``, ``"*7N*.gz"``).
            Barras iniciais são descartadas; não pode conter ``://``. Resulta
            em ``<source_folder>/<pattern>`` (``ConfigPaths.source_file``).
        schema (str): Chave exata do contrato de extração do CDR. Aqui só é
            validada como string não vazia; o suporte ao schema é verificado
            na preparação do lote, pelo orquestrador.
        extracted_folder (str): Pasta do Parquet intermediário
            ``<extracted_folder>/<output_file_prefix>_extracted.parquet``.
            Opcional quando ``materialize_intermediate=False`` é passado
            à API; nesse modo, a pasta não é utilizada como destino.
        transformed_folder (str): Pasta do Parquet padronizado
            ``<transformed_folder>/<output_file_prefix>_transformed.parquet``.
        output_file_prefix (str): Prefixo dos nomes dos arquivos de saída.
            Deve ser um nome simples: sem barras, sem espaços nas bordas, sem
            caracteres de controle, ``\\``, ``.`` ou ``..`` e sem curingas
            (ex.: ``"claro_smp_ericsson"``).

    Chaves opcionais:
        storage_type (str): Tipo de armazenamento das três pastas do item.
            Valores: ``"s3a"`` (padrão, se ausente) ou ``"local"``. Não há
            armazenamento misto no mesmo item.

            - ``"s3a"``: ``bucket/caminho`` ou ``s3a://bucket/caminho``;
              nunca caminho absoluto, ``:`` no primeiro segmento nem outro
              protocolo. O bucket deve casar com ``[a-z0-9][a-z0-9.-]*``.
              Barras finais são removidas.
            - ``"local"``: caminho absoluto Linux sem protocolo
              (``/dados/x``); as saídas usam ``file:///``. Não expande ``~``
              nem resolve links simbólicos.

            Em ambos, as pastas não podem ter espaços nas bordas, curingas,
            caracteres de controle, ``\\`` nem segmentos ``.``/``..``
            (inclusive em percent-encoding, como ``%2e%2e``).
        operator (str | None): Prestadora (coluna ``prestadora``), repassada
            aos extratores. Se ausente, ``None`` ou vazia, é inferida pelo
            extrator a partir do caminho do arquivo de origem.
        service (str | None): Serviço (coluna ``servico``), por exemplo
            ``smp`` ou ``stfc``. Se não informado, é inferido do caminho.
        cdr_type (str | None): Tipo de CDR (coluna ``tipo_cdr``). Se não
            informado, é inferido do caminho.

    ``operator``, ``service`` e ``cdr_type`` aceitam ``str`` ou ``None``;
    string não vazia contendo somente espaços gera ``ValueError``. Os nomes
    não são validados contra um catálogo de valores.

    Exemplo de item (``storage_type`` local)::

        {
            "source_folder": "/dados/claro/smp/ericsson",
            "pattern": "*.parquet",
            "schema": "smp_ericsson_gsm",
            "extracted_folder": "/dados/temp/extracted",
            "transformed_folder": "/dados/temp/transformed",
            "output_file_prefix": "claro_smp_ericsson",
            "storage_type": "local",
            "operator": "claro",
        }

Exemplo:
    >>> get_config({
    ...     "source_folder": "bucket/cdr",
    ...     "pattern": "*.parquet",
    ...     "schema": "meu_schema",
    ...     "extracted_folder": "bucket/extraido",
    ...     "transformed_folder": "bucket/transformado",
    ...     "output_file_prefix": "lote",
    ... }).extracted_file
    's3a://bucket/extraido/lote_extracted.parquet'
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import NamedTuple
from urllib.parse import unquote

logger = logging.getLogger(__name__)

# Chaves obrigatórias de cada item de configuração.
_REQUIRED_KEYS = (
    "source_folder",
    "pattern",
    "schema",
    "extracted_folder",
    "transformed_folder",
    "output_file_prefix",
)
# Chaves opcionais repassadas aos extratores pelo orquestrador.
_METADATA_KEYS = ("operator", "service", "cdr_type")
# Qualquer chave fora deste conjunto é rejeitada em _prepare_config.
_ALLOWED_KEYS = frozenset((*_REQUIRED_KEYS, *_METADATA_KEYS, "storage_type"))


class ConfigPaths(NamedTuple):
    """Caminhos planejados; sua construção não comprova existência ou integridade.

    Attributes:
        schema: Chave exata do contrato de extração.
        source_file: Caminho Spark/Hadoop s3a:// ou file:///, possivelmente com glob.
        extracted_file: Diretório Parquet intermediário ou None no modo direto.
        transformed_file: Diretório Parquet padronizado.
    """

    schema: str
    source_file: str
    extracted_file: str | None
    transformed_file: str


@dataclass(frozen=True)
class _PreparedConfig:
    """Resultado interno da validação de um item de configuração.

    Além dos caminhos públicos, guarda dados que ``get_config`` descarta e que
    o orquestrador (``etl.py``) utiliza.

    Attributes:
        paths: Caminhos planejados do item.
        source_folder: Pasta de origem normalizada, sem o ``pattern``; usada na
            detecção de sobreposição entre origem e destinos.
        operator: Metadado opcional repassado aos extratores.
        service: Metadado opcional repassado aos extratores.
        cdr_type: Metadado opcional repassado aos extratores.
    """

    paths: ConfigPaths
    source_folder: str
    operator: str | None
    service: str | None
    cdr_type: str | None


def _validate_unique_prefixes(configs: Sequence[Mapping[str, object]]) -> None:
    """Rejeita prefixos não vazios repetidos, comparados exatamente.

    Itens que não são mappings e prefixos ausentes, vazios ou de tipo inválido
    são deixados para a validação individual.

    Raises:
        ValueError: Se um prefixo ocorrer mais de uma vez, com seus índices.
    """
    indices: dict[str, list[int]] = {}
    for index, config in enumerate(configs):
        if not isinstance(config, Mapping):
            continue
        prefix = config.get("output_file_prefix")
        if isinstance(prefix, str) and prefix.strip():
            indices.setdefault(prefix, []).append(index)
    duplicates = [
        f"{prefix!r}: índices {positions}"
        for prefix, positions in indices.items()
        if len(positions) > 1
    ]
    if duplicates:
        raise ValueError(f"output_file_prefix duplicado: {'; '.join(duplicates)}.")


def _resolve_minio_setting(name: str, value: str | None, env_name: str) -> str:
    value = value if value is not None else os.environ.get(env_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} ou a variável {env_name} deve ser informada.")
    return value


def load_config(
    path: str,
    *,
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    secure: bool = False,
) -> list[dict[str, object]]:
    """Lê uma lista JSON de configurações e rejeita prefixos repetidos.

    Args:
        path: Caminho absoluto Linux ou URI ``s3a://bucket/chave``.
        endpoint: Hostname e porta opcionais do MinIO, sem protocolo. Se
            omitido, usa ``MINIO_ENDPOINT`` do ambiente.
        access_key: Chave de acesso; se omitida, usa ``MINIO_USER``.
        secret_key: Chave secreta; se omitida, usa ``MINIO_SECRET``.
        secure: Usa HTTPS quando True e HTTP quando False. Os parâmetros
            MinIO são utilizados somente para caminhos S3A.

    Returns:
        Lista de dicionários, sem modificar os valores do arquivo.

    Raises:
        TypeError: Se path não for string ou o JSON não for uma lista de objetos.
        ValueError: Se o caminho ou as opções MinIO forem inválidos ou se
            houver prefixos duplicados.
        OSError: Se houver erro ao ler o arquivo local.

    Notes:
        Exceções de leitura, JSON inválido e erros do SDK MinIO são propagados.
        A validação dos demais campos ocorre individualmente em ``run_etl``.
    """
    if not isinstance(path, str):
        raise TypeError("path deve ser uma string.")

    config_data: object
    if path.startswith("s3a://"):
        bucket, separator, object_name = path[len("s3a://") :].partition("/")
        if not bucket or not separator or not object_name:
            raise ValueError("path S3A deve conter bucket e chave do arquivo.")
        if not isinstance(secure, bool):
            raise TypeError("secure deve ser booleano.")

        endpoint = _resolve_minio_setting("endpoint", endpoint, "MINIO_ENDPOINT")
        access_key = _resolve_minio_setting("access_key", access_key, "MINIO_USER")
        secret_key = _resolve_minio_setting("secret_key", secret_key, "MINIO_SECRET")

        from minio import Minio

        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        response = client.get_object(bucket, object_name)
        try:
            config_data = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
    elif path.startswith("/"):
        with open(path, encoding="utf-8") as file:
            config_data = json.load(file)
    else:
        raise ValueError("path deve ser absoluto Linux ou uma URI s3a://.")

    if not isinstance(config_data, list):
        raise TypeError("O JSON deve conter uma lista de configurações.")
    for index, config in enumerate(config_data):
        if not isinstance(config, dict):
            raise TypeError(
                f"A configuração no índice {index} deve ser um objeto JSON."
            )
    _validate_unique_prefixes(config_data)
    return config_data


def list_files(
    base_folder: str,
    pattern: str = "",
    recursive: bool = False,
    *,
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    secure: bool = False,
) -> list[str]:
    """Lista arquivos lógicos no MinIO, sem percorrer datasets ``.parquet``.

    Diretórios com sufixo literal ``.parquet`` são retornados como uma única
    entrada, independentemente do conteúdo e da quantidade de partições.
    Outras pastas são percorridas somente quando ``recursive=True``.
    Não requer Spark nem lê o conteúdo dos objetos.

    Args:
        base_folder: Pasta contêiner em ``bucket[/prefixo]`` ou
            ``s3a://bucket[/prefixo]``. Não pode ser um dataset ``.parquet``
            nem um caminho dentro dele. Barras finais são removidas.
        pattern: Máscara aplicada ao nome do arquivo ou dataset, sensível a
            maiúsculas, com ``*``, ``?`` e classes ``[]``. Não aceita caminhos
            nem expande alternativas ``{a,b}``. Vazia inclui todos os tipos,
            inclusive arquivos auxiliares fora dos datasets.
        recursive: Se True, busca também em subpastas comuns. Nunca entra
            em datasets, mesmo quando seus nomes não correspondem à máscara.
        endpoint: Hostname e porta opcionais, sem protocolo. Se omitido,
            usa ``MINIO_ENDPOINT``.
        access_key: Chave de acesso; se omitida, usa ``MINIO_USER``.
        secret_key: Chave secreta; se omitida, usa ``MINIO_SECRET``.
        secure: Usa HTTPS quando True; o padrão False usa HTTP.

    Returns:
        URIs ``s3a://`` ordenadas, sem duplicatas ou barra final. Prefixos
        sem objetos ou sem correspondências retornam uma lista vazia.
        Arquivos físicos Parquet fora de datasets são arquivos comuns.

    Raises:
        TypeError: Se os argumentos tiverem tipos inválidos.
        ValueError: Se a base, a máscara ou as opções MinIO forem inválidas,
            ou se o SDK retornar um objeto sem nome.

    Notes:
        Erros do SDK, inclusive durante a paginação, são propagados sem
        retornar resultados parciais. O sufixo não comprova integridade ou
        formato: diretórios sem extensão não são reconhecidos como datasets.
    """
    if not isinstance(base_folder, str):
        raise TypeError("base_folder deve ser uma string.")
    if not isinstance(pattern, str):
        raise TypeError("pattern deve ser uma string.")
    if not isinstance(recursive, bool):
        raise TypeError("recursive deve ser booleano.")
    if not isinstance(secure, bool):
        raise TypeError("secure deve ser booleano.")

    folder = _s3a_folder(base_folder, "base_folder")
    bucket, _, key = folder[len("s3a://") :].partition("/")
    if any(part.endswith(".parquet") for part in key.split("/")):
        raise ValueError(
            "base_folder deve ser uma pasta contêiner, não um dataset "
            ".parquet ou um caminho dentro dele."
        )
    _validate_path(pattern, "pattern", glob=True)
    if "/" in unquote(pattern):
        raise ValueError("pattern deve ser uma máscara de nome, sem caminhos.")

    endpoint = _resolve_minio_setting("endpoint", endpoint, "MINIO_ENDPOINT")
    access_key = _resolve_minio_setting("access_key", access_key, "MINIO_USER")
    secret_key = _resolve_minio_setting("secret_key", secret_key, "MINIO_SECRET")

    from minio import Minio

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    pending = [f"{key}/" if key else ""]
    visited: set[str] = set()
    files: set[str] = set()
    while pending:
        prefix = pending.pop()
        if prefix in visited:
            continue
        visited.add(prefix)
        for item in client.list_objects(bucket, prefix=prefix, recursive=False):
            object_name = item.object_name
            if not object_name:
                raise ValueError("A listagem MinIO retornou um objeto sem nome.")
            if object_name == prefix:
                continue
            logical_key = object_name.rstrip("/")
            name = logical_key.rsplit("/", 1)[-1]
            if item.is_dir and not name.endswith(".parquet"):
                if recursive:
                    pending.append(object_name)
                continue
            # A poda ocorre antes do filtro para não expor partições rejeitadas.
            if not pattern or fnmatchcase(name, pattern):
                files.add(f"s3a://{bucket}/{logical_key}")
    return sorted(files)


def _required_string(config: Mapping[str, object], key: str) -> str:
    """Retorna ``config[key]`` sem alterá-lo, exigindo string não vazia.

    Raises:
        TypeError: Se o valor não for ``str``.
        ValueError: Se o valor for vazio ou contiver somente espaços.
    """
    value = config[key]
    if not isinstance(value, str):
        raise TypeError(f"{key} deve ser uma string.")
    if not value.strip():
        raise ValueError(f"{key} não pode ser vazio ou conter somente espaços.")
    return value


def _validate_path(value: str, key: str, *, glob: bool = False) -> None:
    """Valida um caminho já sem protocolo; não o modifica nem o retorna.

    As regras são aplicadas sobre ``unquote(value)``, de modo que formas
    codificadas em percent-encoding (por exemplo ``%2e%2e``) também sejam
    barradas. O valor original, ainda codificado, é o que segue para os
    caminhos finais.

    Args:
        value: Caminho a validar.
        key: Nome do campo, usado nas mensagens de erro.
        glob: Se ``True``, aceita os curingas ``*?[]{}`` (usado em ``pattern``).

    Raises:
        ValueError: Se houver caracteres de controle, ``\\``, segmentos ``.``
            ou ``..``, ou curingas quando ``glob`` for ``False``.
    """
    decoded = unquote(value)
    if any(ord(char) < 32 or ord(char) == 127 for char in decoded):
        raise ValueError(f"{key} não pode conter caracteres de controle.")
    if "\\" in decoded or any(part in (".", "..") for part in decoded.split("/")):
        raise ValueError(f"{key} não pode conter '\\', '.' ou '..' como segmentos.")
    if not glob and any(char in decoded for char in "*?[]{}"):
        raise ValueError(f"{key} não pode conter curingas.")


def _s3a_folder(value: str, key: str) -> str:
    """Normaliza uma pasta S3A para ``s3a://bucket[/caminho]``.

    Aceita ``s3a://bucket/caminho`` ou ``bucket/caminho``; barras finais são
    removidas. Sem o prefixo ``s3a://``, rejeita caminho iniciado por ``/``,
    outro protocolo e ``:`` no primeiro segmento. O primeiro segmento deve ser
    um bucket que case com ``[a-z0-9][a-z0-9.-]*``.

    Args:
        value: Pasta informada na configuração.
        key: Nome do campo, usado nas mensagens de erro.

    Returns:
        Pasta normalizada com o prefixo ``s3a://``.

    Raises:
        ValueError: Se houver espaços nas bordas, formato incompatível com S3A,
            caminho inválido (ver ``_validate_path``) ou bucket inválido.
    """
    if value != value.strip():
        raise ValueError(f"{key} não pode conter espaços nas bordas.")
    if value.startswith("s3a://"):
        value = value[len("s3a://") :].rstrip("/")
    else:
        if value.startswith("/") or "://" in value or ":" in value.split("/", 1)[0]:
            raise ValueError(f"{key} deve usar o protocolo s3a:// ou bucket/caminho.")
        value = value.strip("/")
    _validate_path(value, key)
    bucket = value.split("/", 1)[0]
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", bucket):
        raise ValueError(f"{key} deve conter um bucket válido e não vazio.")
    return f"s3a://{value}"


def _local_folder(value: str, key: str) -> str:
    """Converte um caminho absoluto Linux em ``file:///caminho``.

    Barras duplicadas e finais são eliminadas. Não expande ``~``, não resolve
    links simbólicos e não consulta o sistema de arquivos.

    Args:
        value: Caminho absoluto Linux, sem protocolo.
        key: Nome do campo, usado nas mensagens de erro.

    Returns:
        Caminho normalizado com o prefixo ``file://``.

    Raises:
        ValueError: Se o caminho não for absoluto, contiver protocolo ou
            espaços nas bordas, ou for inválido (ver ``_validate_path``).
    """
    if not value.startswith("/") or "://" in value:
        raise ValueError(f"{key} deve ser um caminho absoluto Linux sem protocolo.")
    if value != value.strip():
        raise ValueError(f"{key} não pode conter espaços nas bordas.")
    _validate_path(value, key)
    path = "/" + "/".join(part for part in value.split("/") if part)
    # Hadoop Path recebe a string e trata os caracteres literais do caminho.
    return f"file://{path}"


def _join_path(folder: str, name: str) -> str:
    """Concatena pasta e nome, garantindo uma barra entre eles."""
    return f"{folder}{'' if folder.endswith('/') else '/'}{name}"


def _validate_materialize_intermediate(materialize_intermediate: bool) -> None:
    """Valida a seleção do modo sem coerção de strings ou inteiros."""
    if not isinstance(materialize_intermediate, bool):
        message = "materialize_intermediate deve ser bool."
        logger.error(message)
        raise TypeError(message)


def _prepare_config(
    config: Mapping[str, object], *, materialize_intermediate: bool = True
) -> _PreparedConfig:
    """Valida um item completo e monta os caminhos e os dados auxiliares.

    Ordem das validações: tipo mapping, chaves desconhecidas, chaves
    obrigatórias ausentes, ``storage_type`` (``'s3a'`` por padrão), strings
    obrigatórias, metadados, pastas, ``pattern`` e ``output_file_prefix``.
    Os caminhos montados são ``<source_folder>/<pattern>``,
    ``<extracted_folder>/<prefix>_extracted.parquet`` e
    ``<transformed_folder>/<prefix>_transformed.parquet``.

    Args:
        config: Item de configuração; não é modificado.
        materialize_intermediate: Se False, permite omitir extracted_folder
            e retorna extracted_file=None; uma pasta informada é validada.

    Returns:
        ``_PreparedConfig`` com ``ConfigPaths``, a ``source_folder``
        normalizada e os metadados (``None`` quando ausentes).

    Raises:
        TypeError: Se ``config`` não for mapping, ou se ``storage_type``, um
            campo obrigatório ou um metadado não for string (metadados também
            aceitam ``None``).
        ValueError: Se houver chaves desconhecidas ou ausentes,
            ``storage_type`` diferente de ``'s3a'``/``'local'``, valores
            vazios, metadados inválidos ou caminhos incompatíveis.

    Notes:
        ``storage_type`` define uma única função de pasta para as três
        pastas do item; não há armazenamento misto.
    """
    _validate_materialize_intermediate(materialize_intermediate)
    if not isinstance(config, Mapping):
        raise TypeError("Cada configuração deve ser um mapping.")
    unknown = [repr(key) for key in config if key not in _ALLOWED_KEYS]
    if unknown:
        raise ValueError(f"Chaves desconhecidas: {', '.join(unknown)}.")
    required_keys = tuple(
        key
        for key in _REQUIRED_KEYS
        if materialize_intermediate or key != "extracted_folder"
    )
    missing = [key for key in required_keys if key not in config]
    if missing:
        raise ValueError(f"Chaves obrigatórias ausentes: {', '.join(missing)}.")
    storage_type = config.get("storage_type", "s3a")
    if not isinstance(storage_type, str):
        raise TypeError("storage_type deve ser uma string.")
    if storage_type not in ("s3a", "local"):
        raise ValueError("storage_type deve ser 's3a' ou 'local'.")
    values = {key: _required_string(config, key) for key in required_keys}
    if not materialize_intermediate and "extracted_folder" in config:
        values["extracted_folder"] = _required_string(config, "extracted_folder")
    metadata: dict[str, str | None] = {}
    for key in _METADATA_KEYS:
        value = config.get(key)
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{key} deve ser uma string ou None.")
        metadata[key] = value
    # Import adiado: reutiliza a regra dos extratores sem exigir uma sessão Spark.
    # Ela valida apenas tipo e espaços em branco, sem restringir os nomes.
    from teleutils.core.extractors._extractors_utils import validate_metadata_arguments

    validate_metadata_arguments(**metadata)
    build_folder = _s3a_folder if storage_type == "s3a" else _local_folder
    source_folder = build_folder(values["source_folder"], "source_folder")
    extracted_folder = (
        build_folder(values["extracted_folder"], "extracted_folder")
        if "extracted_folder" in values
        else None
    )
    transformed_folder = build_folder(
        values["transformed_folder"], "transformed_folder"
    )
    # Barras iniciais são descartadas para o pattern ser sempre relativo à
    # source_folder.
    pattern = values["pattern"].lstrip("/")
    if not pattern.strip() or "://" in pattern:
        raise ValueError("pattern deve ser um padrão relativo não vazio.")
    _validate_path(pattern, "pattern", glob=True)
    prefix = values["output_file_prefix"]
    _validate_path(prefix, "output_file_prefix")
    if "/" in unquote(prefix) or prefix != prefix.strip():
        raise ValueError(
            "output_file_prefix deve ser um nome sem barras ou espaços nas bordas."
        )
    paths = ConfigPaths(
        schema=values["schema"],
        source_file=_join_path(source_folder, pattern),
        extracted_file=(
            _join_path(extracted_folder, f"{prefix}_extracted.parquet")
            if materialize_intermediate and extracted_folder is not None
            else None
        ),
        transformed_file=_join_path(
            transformed_folder, f"{prefix}_transformed.parquet"
        ),
    )
    return _PreparedConfig(paths, source_folder, **metadata)


def get_config(
    config: Mapping[str, object], *, materialize_intermediate: bool = True
) -> ConfigPaths:
    """Valida um item e constrói caminhos S3A ou locais sem acessar armazenamento.

    Args:
        config: Mapping com source_folder, pattern, schema, extracted_folder,
            transformed_folder e output_file_prefix obrigatórios. Os metadados
            operator, service e cdr_type são opcionais; seguem os extratores.
            storage_type é opcional: 's3a' (padrão) ou 'local'. Em S3A,
            as três pastas aceitam bucket/caminho ou s3a://bucket/caminho,
            nunca caminhos absolutos Linux. Em local, todas devem ser
            caminhos absolutos Linux sem protocolo; as saídas usam file:///.
            Não são aceitas outras chaves nem armazenamentos híbridos no
            mesmo item. O mapping original não é modificado.
        materialize_intermediate: Se False, extracted_folder é opcional e
            extracted_file será None. A pasta informada ainda é validada,
            mas não representa um destino de escrita.

    Returns:
        Caminhos Spark/Hadoop com os sufixos _extracted.parquet e
        _transformed.parquet. Não valida suporte ao schema nem conflitos
        entre itens; essas verificações pertencem à preparação do lote.
        Caminhos locais preservam caracteres literais, sem expansão de
        diretório de trabalho, ~ ou resolução de links simbólicos.

    Raises:
        TypeError: Se o item não for mapping ou um campo tiver tipo inválido.
        ValueError: Se faltarem campos, houver chaves desconhecidas, valores
            vazios, metadados inválidos ou caminhos incompatíveis.
    """
    return _prepare_config(
        config, materialize_intermediate=materialize_intermediate
    ).paths
