from dataclasses import FrozenInstanceError

import pytest

from teleutils.core.orchestrators.config import (
    ConfigPaths,
    _prepare_config,
    _validate_unique_prefixes,
    get_config,
)


@pytest.fixture
def config():
    return {
        "source_folder": "sandbox-sfi/cdr/bruto/algar/stfc/huawei_ngn",
        "pattern": "*.gz",
        "schema": "stfc_huawei_ngn",
        "extracted_folder": "sandbox-sfi/cdr/extraido",
        "transformed_folder": "sandbox-sfi/cdr/transformado",
        "output_file_prefix": "algar_huawei_ngn",
    }


def test_paths_and_input_are_preserved(config):
    original = config.copy()
    assert get_config(config) == ConfigPaths(
        "stfc_huawei_ngn",
        "s3a://sandbox-sfi/cdr/bruto/algar/stfc/huawei_ngn/*.gz",
        "s3a://sandbox-sfi/cdr/extraido/algar_huawei_ngn_extracted.parquet",
        "s3a://sandbox-sfi/cdr/transformado/algar_huawei_ngn_transformed.parquet",
    )
    assert config == original


def test_unique_prefix_validation_reports_all_duplicates_and_indices():
    configs = [
        {"output_file_prefix": "a"},
        {"output_file_prefix": "b"},
        {"output_file_prefix": "a"},
        {"output_file_prefix": "b"},
        {"output_file_prefix": "a"},
    ]
    with pytest.raises(ValueError) as error:
        _validate_unique_prefixes(configs)
    assert "'a': índices [0, 2, 4]" in str(error.value)
    assert "'b': índices [1, 3]" in str(error.value)


def test_unique_prefix_validation_leaves_invalid_items_to_individual_validation():
    _validate_unique_prefixes(
        [
            None,
            {},
            {"output_file_prefix": []},
            {"output_file_prefix": ""},
            {"output_file_prefix": ""},
            {"output_file_prefix": "a"},
            {"output_file_prefix": "A"},
        ]
    )


@pytest.mark.parametrize("protocol", ["", "s3a://"])
def test_folder_boundaries_and_pattern(config, protocol):
    config["source_folder"] = f"{protocol}bucket/folder///"
    config["pattern"] = "/sub/file[0-9]?*.gz"
    assert get_config(config).source_file == "s3a://bucket/folder/sub/file[0-9]?*.gz"


def test_bare_folder_slashes_and_significant_key_characters(config):
    config["source_folder"] = "bucket/key with spaces/dir+@%25//sub/"
    assert get_config(config).source_file == (
        "s3a://bucket/key with spaces/dir+@%25//sub/*.gz"
    )


@pytest.mark.parametrize(
    "key",
    [
        "source_folder",
        "pattern",
        "schema",
        "extracted_folder",
        "transformed_folder",
        "output_file_prefix",
    ],
)
def test_missing_required_key(config, key):
    del config[key]
    with pytest.raises(ValueError, match=key):
        get_config(config)


@pytest.mark.parametrize("value", [None, 0, True, [], {}])
@pytest.mark.parametrize(
    "key",
    [
        "source_folder",
        "pattern",
        "schema",
        "extracted_folder",
        "transformed_folder",
        "output_file_prefix",
    ],
)
def test_invalid_required_types(config, key, value):
    config[key] = value
    with pytest.raises(TypeError, match=key):
        get_config(config)


@pytest.mark.parametrize("value", ["", " ", "\t"])
@pytest.mark.parametrize(
    "key",
    [
        "source_folder",
        "pattern",
        "schema",
        "extracted_folder",
        "transformed_folder",
        "output_file_prefix",
    ],
)
def test_empty_required_values(config, key, value):
    config[key] = value
    with pytest.raises(ValueError, match=key):
        get_config(config)


@pytest.mark.parametrize("keep_correct", [True, False])
def test_misspelled_prefix_is_unknown(config, keep_correct):
    config["output_file_preffix"] = config["output_file_prefix"]
    if not keep_correct:
        del config["output_file_prefix"]
    with pytest.raises(ValueError, match="Chaves desconhecidas"):
        get_config(config)


@pytest.mark.parametrize("key", ["extra", 1])
def test_unknown_keys(config, key):
    config[key] = "value"
    with pytest.raises(ValueError, match="Chaves desconhecidas"):
        get_config(config)


@pytest.mark.parametrize("value", [None, [], "config", 1])
def test_item_must_be_mapping(value):
    with pytest.raises(TypeError, match="mapping"):
        get_config(value)


@pytest.mark.parametrize(
    "folder",
    [
        "/",
        "s3a://",
        "s3a:///bucket",
        "s3://bucket/path",
        "file:/tmp/input",
        "hdfs://host/path",
        " bucket/path",
        "bucket/path ",
        "bucket/path\nname",
        "bucket/../path",
        "bucket/%2e%2e/path",
        "bucket/a\\b",
        "bucket/*.gz",
    ],
)
def test_invalid_folders(config, folder):
    config["source_folder"] = folder
    with pytest.raises(ValueError, match="source_folder"):
        get_config(config)


@pytest.mark.parametrize(
    "prefix", ["/", "a/b", ".", "..", "a*", "a?", " a", "a\n", "a%2Fb"]
)
def test_invalid_prefix(config, prefix):
    config["output_file_prefix"] = prefix
    with pytest.raises(ValueError, match="output_file_prefix"):
        get_config(config)


@pytest.mark.parametrize("pattern", ["/", "../*", "sub/../*", "s3a://bucket/*", "a\n*"])
def test_invalid_pattern(config, pattern):
    config["pattern"] = pattern
    with pytest.raises(ValueError, match="pattern"):
        get_config(config)


@pytest.mark.parametrize("key", ["operator", "service", "cdr_type"])
@pytest.mark.parametrize("value", [None, "", "custom value"])
def test_optional_metadata(config, key, value):
    config[key] = value
    assert getattr(_prepare_config(config), key) == value


@pytest.mark.parametrize("key", ["operator", "service", "cdr_type"])
@pytest.mark.parametrize("value,error", [(1, TypeError), (" ", ValueError)])
def test_invalid_metadata(config, key, value, error):
    config[key] = value
    with pytest.raises(error, match=key):
        get_config(config)


def test_schema_not_normalized_and_preparation_is_frozen(config):
    config["schema"] = " Custom "
    prepared = _prepare_config(config)
    assert prepared.paths.schema == " Custom "
    with pytest.raises(FrozenInstanceError):
        prepared.service = "other"


def test_explicit_s3a_matches_default(config):
    default_paths = get_config(config)
    config["storage_type"] = "s3a"
    assert get_config(config) == default_paths


@pytest.mark.parametrize("value", [None, True, 0, [], {}])
def test_storage_type_must_be_string(config, value):
    config["storage_type"] = value
    with pytest.raises(TypeError, match="storage_type"):
        get_config(config)


@pytest.mark.parametrize(
    "value", ["", " ", "S3A", "LOCAL", " local ", "s3", "file", "hdfs"]
)
def test_unknown_storage_type(config, value):
    config["storage_type"] = value
    with pytest.raises(ValueError, match="storage_type"):
        get_config(config)


def local_config(config):
    return {
        **config,
        "storage_type": "local",
        "source_folder": "/dados/bruto/",
        "extracted_folder": "/dados//extraido///",
        "transformed_folder": "/dados/transformado",
    }


def test_local_paths_metadata_globs_and_no_mutation(config):
    config = local_config(config)
    config.update(
        pattern="sub/file[0-9]?*.gz", operator="algar", service="", cdr_type=None
    )
    original = config.copy()
    prepared = _prepare_config(config)
    assert prepared.paths == ConfigPaths(
        "stfc_huawei_ngn",
        "file:///dados/bruto/sub/file[0-9]?*.gz",
        "file:///dados/extraido/algar_huawei_ngn_extracted.parquet",
        "file:///dados/transformado/algar_huawei_ngn_transformed.parquet",
    )
    assert prepared.source_folder == "file:///dados/bruto"
    assert (prepared.operator, prepared.service, prepared.cdr_type) == (
        "algar",
        "",
        None,
    )
    assert config == original


def test_local_root_is_preserved_without_access(config):
    config = local_config(config)
    config["source_folder"] = "/"
    assert get_config(config).source_file == "file:///*.gz"


def test_local_names_are_literal(config):
    config = local_config(config)
    config["source_folder"] = "/dados/espaço +@#%25"
    assert get_config(config).source_file == "file:///dados/espaço +@#%25/*.gz"


@pytest.mark.parametrize(
    "key", ["source_folder", "extracted_folder", "transformed_folder"]
)
@pytest.mark.parametrize("storage", ["s3a", None])
def test_s3a_rejects_absolute_local_folders(config, key, storage):
    if storage is not None:
        config["storage_type"] = storage
    config[key] = "/dados/cdr"
    with pytest.raises(ValueError, match=key):
        get_config(config)


@pytest.mark.parametrize(
    "key", ["source_folder", "extracted_folder", "transformed_folder"]
)
@pytest.mark.parametrize(
    "folder",
    [
        "dados/cdr",
        "./dados",
        "~/dados",
        "s3a://bucket/cdr",
        "s3://bucket/cdr",
        "file:///dados",
        "file:/dados",
        "hdfs://host/dados",
        "https://host/dados",
        "C:\\dados",
        "/dados/../cdr",
        "/dados/./cdr",
        "/dados/%2e%2e/cdr",
        "/dados/a\\b",
        "/dados/*.gz",
        "/dados/\n",
        " /dados",
        "/dados ",
    ],
)
def test_local_rejects_incompatible_or_unsafe_folder(config, key, folder):
    config = local_config(config)
    config[key] = folder
    with pytest.raises(ValueError, match=key):
        get_config(config)


@pytest.mark.parametrize(
    "key", ["source_folder", "extracted_folder", "transformed_folder"]
)
@pytest.mark.parametrize(
    "folder", ["file:///dados", "hdfs://host/data", "https://host/data"]
)
def test_s3a_rejects_other_protocols_in_all_folders(config, key, folder):
    config[key] = folder
    with pytest.raises(ValueError, match=key):
        get_config(config)
