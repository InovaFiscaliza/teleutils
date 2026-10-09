from __future__ import annotations

from types import MappingProxyType
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from teleutils.core.extractors.parquet_extractors import CDRParquetExtractor
from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    TEXT_DEFAULT_SCHEMAS,
    resolve_cdr_schema,
)
from teleutils.core.extractors.text_extractors import CDRTextExtractor


@pytest.mark.parametrize("schemas", [PARQUET_DEFAULT_SCHEMAS, TEXT_DEFAULT_SCHEMAS])
def test_resolve_cdr_schema_returns_original_contract(schemas):
    key = next(iter(schemas))
    assert resolve_cdr_schema(schemas, key) is schemas[key]


def test_resolve_cdr_schema_accepts_read_only_mapping():
    contract = object()
    schemas = {"layout": contract}
    assert resolve_cdr_schema(MappingProxyType(schemas), "layout") is contract
    assert schemas == {"layout": contract}


@pytest.mark.parametrize("schemas", [{}, {"z_layout": object(), "a_layout": object()}])
def test_resolve_cdr_schema_preserves_missing_key_message(schemas):
    with pytest.raises(ValueError) as captured:
        resolve_cdr_schema(schemas, "missing")

    assert str(captured.value) == (
        f"Schema 'missing' não encontrado. Schemas disponíveis: {list(schemas)}"
    )


@pytest.mark.parametrize("key", [None, 1, [], {}, True])
def test_resolve_cdr_schema_rejects_non_string_keys(key):
    with pytest.raises(TypeError) as captured:
        resolve_cdr_schema({}, key)

    assert str(captured.value) == (
        f"cdr_schema deve ser uma string. Recebido: {type(key).__name__}"
    )


@pytest.mark.parametrize("key", ["", " ", " Layout ", "LAYOUT"])
def test_resolve_cdr_schema_keeps_exact_custom_keys(key):
    contract = object()
    assert resolve_cdr_schema({key: contract}, key) is contract


@pytest.mark.parametrize("key", ["", " ", " layout ", "LAYOUT"])
def test_resolve_cdr_schema_does_not_normalize_keys(key):
    with pytest.raises(ValueError):
        resolve_cdr_schema({"layout": object()}, key)


@pytest.mark.parametrize("extractor_class", [CDRParquetExtractor, CDRTextExtractor])
@pytest.mark.parametrize(
    ("key", "error_type"), [("missing", ValueError), ([], TypeError)]
)
def test_extractors_reject_invalid_schema_before_reading(
    extractor_class, key, error_type
):
    spark = Mock(spec=SparkSession)
    extractor = extractor_class(spark)

    with pytest.raises(error_type):
        extractor.extract("entrada", "saida", key)

    assert spark.mock_calls == []
