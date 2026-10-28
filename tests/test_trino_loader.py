from unittest.mock import MagicMock, patch

import pytest

from teleutils._config import PRIMARY_KEY_COLUMNS, TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader


@pytest.fixture
def spark():
    session = MagicMock()
    session.read.parquet.return_value.columns = [
        column for column, _ in TARGET_SCHEMA.values()
    ]
    session.catalog.tableExists.return_value = True
    return session


def test_merge_uses_default_keys_and_cleans_view(spark, caplog):
    with caplog.at_level("INFO"):
        TrinoLoader(spark).upsert_iceberg("/dados/parquet", "catalogo.schema.chamadas")

    spark.read.parquet.assert_called_once_with("/dados/parquet")
    df = spark.read.parquet.return_value
    df.dropDuplicates.assert_called_once_with(PRIMARY_KEY_COLUMNS)
    view = df.dropDuplicates.return_value.createOrReplaceTempView.call_args.args[0]
    query = spark.sql.call_args.args[0]
    assert "MERGE INTO `catalogo`.`schema`.`chamadas`" in query
    assert f"USING `{view}`" in query
    for key in PRIMARY_KEY_COLUMNS:
        assert f"target.`{key}` <=> source.`{key}`" in query
    assert "WHEN MATCHED THEN UPDATE SET *" in query
    assert "WHEN NOT MATCHED THEN INSERT *" in query
    spark.catalog.dropTempView.assert_called_once_with(view)
    assert "Atualizando e inserindo registros" in caplog.text


@pytest.mark.parametrize("mode", ["merge", "insert_only", "append"])
def test_missing_table_is_created_without_replacement(spark, mode):
    spark.catalog.tableExists.return_value = False
    TrinoLoader(spark).upsert_iceberg(
        "/dados/parquet", "catalogo.schema.chamadas", mode=mode
    )

    df = spark.read.parquet.return_value
    if mode == "append":
        df.dropDuplicates.assert_not_called()
        writer = df.writeTo
    else:
        df.dropDuplicates.assert_called_once_with(PRIMARY_KEY_COLUMNS)
        writer = df.dropDuplicates.return_value.writeTo
    writer.assert_called_once_with("`catalogo`.`schema`.`chamadas`")
    writer.return_value.using.assert_called_once_with("iceberg")
    writer.return_value.using.return_value.create.assert_called_once_with()
    writer.return_value.using.return_value.createOrReplace.assert_not_called()
    writer.return_value.append.assert_not_called()
    spark.sql.assert_not_called()


def test_custom_keys_are_quoted_and_views_are_unique(spark):
    spark.read.parquet.return_value.columns.append("chave`especial")
    loader = TrinoLoader(spark)
    for _ in range(2):
        loader.upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", ["chave`especial"]
        )

    queries = [call.args[0] for call in spark.sql.call_args_list]
    assert "target.`chave``especial` <=> source.`chave``especial`" in queries[0]
    assert queries[0] != queries[1]


@pytest.mark.parametrize("mode", ["merge", "insert_only"])
def test_merge_failure_is_logged_and_cleans_view(spark, caplog, mode):
    spark.sql.side_effect = RuntimeError("Falha no MERGE")
    with pytest.raises(RuntimeError, match="Falha no MERGE"):
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", mode=mode
        )

    spark.catalog.dropTempView.assert_called_once()
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text


@pytest.mark.parametrize("keys", [[], [""], [" "], [None], ["nu_origem", "nu_origem"]])
@pytest.mark.parametrize("mode", ["merge", "insert_only"])
def test_invalid_keys_fail_before_reading(spark, keys, mode):
    with pytest.raises(ValueError, match="primary_keys"):
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", keys, mode=mode
        )
    spark.read.parquet.assert_not_called()


@pytest.mark.parametrize("mode", ["merge", "insert_only"])
def test_missing_key_fails_before_writing(spark, mode):
    with pytest.raises(ValueError, match="ausentes.*inexistente"):
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", ["inexistente"], mode=mode
        )
    spark.sql.assert_not_called()
    spark.catalog.tableExists.assert_not_called()


@pytest.mark.parametrize("column", [column for column, _ in TARGET_SCHEMA.values()])
@pytest.mark.parametrize("mode", ["merge", "insert_only", "append"])
def test_missing_target_column_fails_before_writing(spark, column, caplog, mode):
    df = spark.read.parquet.return_value
    df.columns.remove(column)
    with pytest.raises(ValueError, match="Colunas finais de TARGET_SCHEMA") as error:
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", mode=mode
        )

    assert column in str(error.value)
    df.dropDuplicates.assert_not_called()
    spark.catalog.tableExists.assert_not_called()
    spark.sql.assert_not_called()
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text


def test_source_names_do_not_replace_final_names(spark):
    spark.read.parquet.return_value.columns = list(TARGET_SCHEMA)
    with pytest.raises(ValueError, match="nu_referencia"):
        TrinoLoader(spark).upsert_iceberg("/dados/parquet", "catalogo.schema.chamadas")
    spark.catalog.tableExists.assert_not_called()


def test_all_missing_target_columns_are_reported(spark):
    spark.read.parquet.return_value.columns.remove("nu_referencia")
    spark.read.parquet.return_value.columns.remove("ic_origem_valido")
    with pytest.raises(ValueError) as error:
        TrinoLoader(spark).upsert_iceberg("/dados/parquet", "catalogo.schema.chamadas")
    assert "nu_referencia" in str(error.value)
    assert "ic_origem_valido" in str(error.value)


@pytest.mark.parametrize("table", ["", "catalogo..chamadas", "a.b.c.d"])
def test_invalid_table_fails_before_reading(spark, table):
    with pytest.raises(ValueError, match="target_table"):
        TrinoLoader(spark).upsert_iceberg("/dados/parquet", table)
    spark.read.parquet.assert_not_called()


@pytest.mark.parametrize("mode", ["merge", "insert_only"])
def test_explicit_merge_modes_use_null_safe_keys(spark, mode):
    TrinoLoader(spark).upsert_iceberg(
        "/dados/parquet", "catalogo.schema.chamadas", mode=mode
    )

    df = spark.read.parquet.return_value
    df.dropDuplicates.assert_called_once_with(PRIMARY_KEY_COLUMNS)
    view = df.dropDuplicates.return_value.createOrReplaceTempView.call_args.args[0]
    spark.sql.assert_called_once()
    query = spark.sql.call_args.args[0]
    assert "MERGE INTO `catalogo`.`schema`.`chamadas`" in query
    assert f"USING `{view}`" in query
    for key in PRIMARY_KEY_COLUMNS:
        assert f"target.`{key}` <=> source.`{key}`" in query
    assert "WHEN NOT MATCHED THEN INSERT *" in query
    if mode == "merge":
        assert "WHEN MATCHED THEN UPDATE SET *" in query
    else:
        assert "WHEN MATCHED" not in query
        assert "UPDATE" not in query
    spark.catalog.dropTempView.assert_called_once_with(view)
    df.writeTo.assert_not_called()
    df.dropDuplicates.return_value.writeTo.assert_not_called()


@pytest.mark.parametrize(
    "keys", [PRIMARY_KEY_COLUMNS, [], [""], ["inexistente"], ["nu_origem", "nu_origem"]]
)
def test_append_writes_all_rows_without_using_keys(spark, keys):
    TrinoLoader(spark).upsert_iceberg(
        "/dados/parquet", "catalogo.schema.chamadas", keys, mode="append"
    )

    df = spark.read.parquet.return_value
    df.dropDuplicates.assert_not_called()
    df.writeTo.assert_called_once_with("`catalogo`.`schema`.`chamadas`")
    df.writeTo.return_value.append.assert_called_once_with()
    df.writeTo.return_value.create.assert_not_called()
    df.writeTo.return_value.using.assert_not_called()
    df.createOrReplaceTempView.assert_not_called()
    spark.sql.assert_not_called()
    spark.table.assert_not_called()
    spark.catalog.dropTempView.assert_not_called()


def test_append_failure_is_logged_and_propagated(spark, caplog):
    df = spark.read.parquet.return_value
    df.writeTo.return_value.append.side_effect = RuntimeError("Falha no append")
    with pytest.raises(RuntimeError, match="Falha no append"):
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", mode="append"
        )
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text
    spark.sql.assert_not_called()


@pytest.mark.parametrize("mode", ["", "MERGE", "overwrite", None, 1, []])
def test_invalid_mode_fails_before_reading(spark, mode, caplog):
    with pytest.raises(ValueError, match="mode"):
        TrinoLoader(spark).upsert_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas", mode=mode
        )
    spark.read.parquet.assert_not_called()
    spark.catalog.tableExists.assert_not_called()
    spark.sql.assert_not_called()
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text


@pytest.mark.parametrize("table_exists", [True, False])
def test_append_alias_delegates_to_upsert_with_append_mode(spark, table_exists):
    spark.catalog.tableExists.return_value = table_exists
    loader = TrinoLoader(spark)
    with patch.object(loader, "upsert_iceberg", wraps=loader.upsert_iceberg) as upsert:
        result = loader.append_to_iceberg("/dados/parquet", "catalogo.schema.chamadas")

    upsert.assert_called_once_with(
        "/dados/parquet", "catalogo.schema.chamadas", mode="append"
    )
    assert result is None
    df = spark.read.parquet.return_value
    df.dropDuplicates.assert_not_called()
    df.writeTo.assert_called_once_with("`catalogo`.`schema`.`chamadas`")
    if table_exists:
        df.writeTo.return_value.append.assert_called_once_with()
    else:
        df.writeTo.return_value.using.assert_called_once_with("iceberg")
        df.writeTo.return_value.using.return_value.create.assert_called_once_with()
    spark.sql.assert_not_called()


def test_append_alias_propagates_errors(spark, caplog):
    spark.read.parquet.return_value.writeTo.return_value.append.side_effect = (
        RuntimeError("Falha no append")
    )
    with pytest.raises(RuntimeError, match="Falha no append"):
        TrinoLoader(spark).append_to_iceberg(
            "/dados/parquet", "catalogo.schema.chamadas"
        )
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text
