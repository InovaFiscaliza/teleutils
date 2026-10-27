"""Validação de configurações ETL e construção de caminhos, sem acesso a dados.

Valida um item de configuração (mapping) e deriva os caminhos Spark/Hadoop da
origem, do Parquet extraído e do Parquet transformado. Nenhuma leitura ou
escrita em armazenamento é feita: os caminhos são apenas planejados.

Fluxo de ``_prepare_config``:
    mapping de entrada
      ↓ tipo, chaves desconhecidas/ausentes e ``storage_type``
      ↓ campos obrigatórios (strings não vazias) e metadados
      ↓ pastas (``s3a://`` ou ``file://``), ``pattern`` e ``output_file_prefix``
      ↓ ``ConfigPaths`` + ``_PreparedConfig``

Principais componentes:
    - ``get_config``: API pública; retorna somente ``ConfigPaths``.
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

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import NamedTuple
from urllib.parse import unquote

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
        extracted_file: Diretório Parquet intermediário.
        transformed_file: Diretório Parquet padronizado.
    """

    schema: str
    source_file: str
    extracted_file: str
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


def _prepare_config(config: Mapping[str, object]) -> _PreparedConfig:
    """Valida um item completo e monta os caminhos e os dados auxiliares.

    Ordem das validações: tipo mapping, chaves desconhecidas, chaves
    obrigatórias ausentes, ``storage_type`` (``'s3a'`` por padrão), strings
    obrigatórias, metadados, pastas, ``pattern`` e ``output_file_prefix``.
    Os caminhos montados são ``<source_folder>/<pattern>``,
    ``<extracted_folder>/<prefix>_extracted.parquet`` e
    ``<transformed_folder>/<prefix>_transformed.parquet``.

    Args:
        config: Item de configuração; não é modificado.

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
    if not isinstance(config, Mapping):
        raise TypeError("Cada configuração deve ser um mapping.")
    unknown = [repr(key) for key in config if key not in _ALLOWED_KEYS]
    if unknown:
        raise ValueError(f"Chaves desconhecidas: {', '.join(unknown)}.")
    missing = [key for key in _REQUIRED_KEYS if key not in config]
    if missing:
        raise ValueError(f"Chaves obrigatórias ausentes: {', '.join(missing)}.")
    storage_type = config.get("storage_type", "s3a")
    if not isinstance(storage_type, str):
        raise TypeError("storage_type deve ser uma string.")
    if storage_type not in ("s3a", "local"):
        raise ValueError("storage_type deve ser 's3a' ou 'local'.")
    values = {key: _required_string(config, key) for key in _REQUIRED_KEYS}
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
    extracted_folder = build_folder(values["extracted_folder"], "extracted_folder")
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
        extracted_file=_join_path(extracted_folder, f"{prefix}_extracted.parquet"),
        transformed_file=_join_path(
            transformed_folder, f"{prefix}_transformed.parquet"
        ),
    )
    return _PreparedConfig(paths, source_folder, **metadata)


def get_config(config: Mapping[str, object]) -> ConfigPaths:
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
    return _prepare_config(config).paths
