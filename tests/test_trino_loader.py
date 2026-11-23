from inspect import signature
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader

SOURCE = "/dados/parquet"
TARGET = "catalogo.schema.chamadas"
QUOTED = "`catalogo`.`schema`.`chamadas`"


@pytest.fixture
def spark(monkeypatch):
    monkeypatch.setattr(F, "col", MagicMock())
    monkeypatch.setattr(F, "when", MagicMock())
    monkeypatch.setattr(F, "max", MagicMock())
    session = MagicMock()
    schema = T.StructType(
        [T.StructField(name, kind) for name, kind in TARGET_SCHEMA.values()]
    )
    df = session.read.parquet.return_value
    df.columns = schema.names
    df.schema = schema
    session.table.return_value.schema = schema
    origins = df.select.return_value.distinct.return_value.persist.return_value
    origins.agg.return_value.first.return_value = (0,)
    session.catalog.tableExists.return_value = True
    session.sql.return_value.isEmpty.return_value = True
    return session


def test_new_origins_are_inserted_without_delete(spark):
    TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    spark.read.parquet.assert_called_once_with(SOURCE)
    spark.catalog.tableExists.assert_called_once_with(QUOTED)
    spark.table.assert_called_once_with(QUOTED)
    spark.sql.assert_called_once()
    spark.read.parquet.return_value.writeTo.assert_called_once_with(QUOTED)
    spark.read.parquet.return_value.writeTo.return_value.append.assert_called_once_with()
    spark.catalog.dropTempView.assert_called_once()


def test_missing_table_is_created_without_replacement(spark):
    spark.catalog.tableExists.return_value = False
    TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    writer = spark.read.parquet.return_value.writeTo.return_value
    writer.using.assert_called_once_with("iceberg")
    writer.using.return_value.create.assert_called_once_with()
    writer.append.assert_not_called()
    spark.sql.assert_not_called()


def test_empty_source_does_not_access_target(spark):
    df = spark.read.parquet.return_value
    origins = df.select.return_value.distinct.return_value.persist.return_value
    origins.agg.return_value.first.return_value = (None,)
    TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    spark.catalog.tableExists.assert_not_called()
    spark.sql.assert_not_called()
    df.writeTo.assert_not_called()
    origins.unpersist.assert_called_once_with()


@pytest.mark.parametrize("column", ["no_arquivo_origem", "nu_referencia"])
def test_missing_required_column_fails_before_writing(spark, column):
    spark.read.parquet.return_value.columns = [
        name for name, _ in TARGET_SCHEMA.values() if name != column
    ]
    with pytest.raises(ValueError, match=column):
        TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    spark.catalog.tableExists.assert_not_called()
    spark.sql.assert_not_called()


@pytest.mark.parametrize(
    "target_schema",
    [
        T.StructType(),
        T.StructType([T.StructField("no_arquivo_origem", T.IntegerType())]),
    ],
)
def test_invalid_target_origin_is_rejected(spark, target_schema):
    spark.table.return_value.schema = target_schema
    with pytest.raises((TypeError, ValueError), match="Destino:.*no_arquivo_origem"):
        TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    spark.sql.assert_not_called()
    spark.read.parquet.return_value.writeTo.assert_not_called()


@pytest.mark.parametrize("table", ["", "catalogo..chamadas", "a.b.c.d"])
def test_invalid_table_fails_before_reading(spark, table):
    with pytest.raises(ValueError, match="target_table"):
        TrinoLoader(spark).upsert_iceberg(SOURCE, table)
    spark.read.parquet.assert_not_called()


@pytest.mark.parametrize("phase", ["check", "delete", "insert"])
def test_failure_is_propagated_and_resources_released(spark, caplog, phase):
    df = spark.read.parquet.return_value
    origins = df.select.return_value.distinct.return_value.persist.return_value
    writer = df.writeTo.return_value
    error = RuntimeError(f"Falha: {phase}")
    check, delete = MagicMock(), MagicMock()
    check.isEmpty.return_value = False
    if phase == "check":
        check.isEmpty.side_effect = error
    elif phase == "delete":
        delete = error
    else:
        writer.append.side_effect = error
    spark.sql.side_effect = [check, delete]
    with pytest.raises(RuntimeError) as exc_info:
        TrinoLoader(spark).upsert_iceberg(SOURCE, TARGET)
    assert exc_info.value is error
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text
    if phase == "insert":
        writer.append.assert_called_once_with()
        assert "Falha parcial: DELETE concluído" in caplog.text
    else:
        df.writeTo.assert_not_called()
    assert spark.sql.call_count == (1 if phase == "check" else 2)
    spark.catalog.dropTempView.assert_called_once_with(
        origins.createOrReplaceTempView.call_args.args[0]
    )
    origins.unpersist.assert_called_once_with()


def test_public_signature_exposes_only_source_and_target():
    assert list(signature(TrinoLoader.upsert_iceberg).parameters) == [
        "self",
        "source_file",
        "target_table",
    ]
    assert not hasattr(TrinoLoader, "append_to_iceberg")
