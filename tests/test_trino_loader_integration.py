import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader
from teleutils.core.loaders.trino_loader import _CONTROL_SCHEMA

_OLD_LOAD = datetime(2020, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)

pytestmark = pytest.mark.skipif(
    not (os.environ.get("ICEBERG_TESTS") or os.environ.get("ICEBERG_TEST_JARS")),
    reason="Requer runtime Iceberg e H2: defina ICEBERG_TEST_JARS ou ICEBERG_TESTS=1.",
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        builder = (
            SparkSession.Builder()
            .master("local[2]")
            .appName("teleutils-loader-iceberg-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.test.catalog-impl",
                "org.apache.iceberg.jdbc.JdbcCatalog",
            )
            .config(
                "spark.sql.catalog.test.uri", "jdbc:h2:mem:teleutils;DB_CLOSE_DELAY=-1"
            )
            .config(
                "spark.sql.catalog.test.warehouse",
                str(tmp_path_factory.mktemp("iceberg")),
            )
        )
        jars = os.environ.get("ICEBERG_TEST_JARS")
        if jars:
            builder = builder.config("spark.jars", jars)
        session = builder.getOrCreate()
        try:
            session.sql("CREATE NAMESPACE IF NOT EXISTS test.loads")
            yield session
        finally:
            session.stop()


@pytest.fixture(params=["table", "parquet"])
def tables(spark, request, tmp_path):
    suffix = uuid4().hex
    target, control = f"test.loads.data_{suffix}", f"test.loads.control_{suffix}"
    if request.param == "parquet":
        control = (tmp_path / "control.parquet").as_uri()
    yield target, control
    for table in (target, control):
        if not table.startswith("file://"):
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def write_source(spark, path, records):
    schema = T.StructType(
        [
            T.StructField(name, data_type, True)
            for name, data_type in TARGET_SCHEMA.values()
        ]
    )
    spark.createDataFrame(
        [
            {"no_arquivo_origem": origin, "nu_referencia": key, "no_bilhetador": value}
            for origin, key, value in records
        ],
        schema,
    ).write.parquet(str(path))
    return str(path)


def read_control(spark, table):
    df = (
        spark.read.parquet(table) if table.startswith("file://") else spark.table(table)
    )
    assert df.columns == _CONTROL_SCHEMA.names
    assert isinstance(df.schema["dh_carga"].dataType, T.TimestampNTZType)
    rows = df.collect()
    assert len({row.no_arquivo_origem for row in rows}) == len(rows)
    return {row.no_arquivo_origem: row.dh_carga for row in rows}


def write_control(df, control):
    if control.startswith("file://"):
        df.write.parquet(control)
    else:
        df.writeTo(control).using("iceberg").create()


@pytest.mark.parametrize("mode", ["merge", "insert_only", "append"])
def test_persistent_control_filters_and_records_all_write_paths(
    spark, tables, tmp_path, caplog, mode
):
    target, control = tables
    loader = TrinoLoader(spark)
    first = write_source(spark, tmp_path / "first", [("a'`.csv", "1", "old")])
    with caplog.at_level("INFO"):
        loader.upsert_iceberg(
            first, target, ["nu_referencia"], mode=mode, control_table=control
        )
    first_control = read_control(spark, control)
    assert set(first_control) == {"a'`.csv"}
    assert "não existe" in caplog.text
    second = write_source(
        spark,
        tmp_path / "second",
        [("a'`.csv", "1", "updated"), ("new.csv", "2", "new")],
    )
    with caplog.at_level("INFO"):
        loader.upsert_iceberg(
            second, target, ["nu_referencia"], mode=mode, control_table=control
        )
    actual = {
        row.nu_referencia: row.no_bilhetador for row in spark.table(target).collect()
    }
    assert actual == {"1": "updated" if mode == "merge" else "old", "2": "new"}
    assert spark.table(target).count() == 2
    second_control = read_control(spark, control)
    assert set(second_control) == {"a'`.csv", "new.csv"}
    if mode == "merge":
        assert second_control["a'`.csv"] > first_control["a'`.csv"]
        assert "Recarregando origem" not in caplog.text
        assert "linhas não carregadas" not in caplog.text
    else:
        assert second_control["a'`.csv"] == first_control["a'`.csv"]
        summary_lines = [
            line for line in caplog.text.splitlines() if "linhas não carregadas" in line
        ]
        assert len(summary_lines) == 1
        assert second in summary_lines[0]
        assert "a'`.csv" not in summary_lines[0]
        with caplog.at_level("INFO"):
            caplog.clear()
            loader.upsert_iceberg(
                second, target, ["nu_referencia"], mode=mode, control_table=control
            )
        assert read_control(spark, control) == second_control
        assert spark.table(target).count() == 2
        assert (
            sum("linhas não carregadas" in line for line in caplog.text.splitlines())
            == 1
        )
        assert "Nenhuma origem elegível" not in caplog.text


@pytest.mark.parametrize("backend", ["table", "parquet"])
def test_many_previously_loaded_files_produce_one_summary_line(
    spark, tmp_path, caplog, backend
):
    target = f"test.loads.log_{uuid4().hex}"
    control = (
        f"test.loads.control_{uuid4().hex}"
        if backend == "table"
        else (tmp_path / f"control-{uuid4().hex}.parquet").as_uri()
    )
    existing = [f"old-{index}.csv" for index in range(20)]
    source = write_source(
        spark,
        tmp_path / f"source-{backend}",
        [(name, str(index), "old") for index, name in enumerate(existing)]
        + [("new.csv", "new", "new")],
    )
    write_control(
        spark.createDataFrame(
            [(name, _OLD_LOAD) for name in existing], _CONTROL_SCHEMA
        ),
        control,
    )
    try:
        with caplog.at_level("INFO"):
            TrinoLoader(spark).upsert_iceberg(
                source, target, ["nu_referencia"], mode="append", control_table=control
            )
        summaries = [
            line for line in caplog.text.splitlines() if "linhas não carregadas" in line
        ]
        assert len(summaries) == 1
        assert source in summaries[0]
        assert all(name not in summaries[0] for name in existing)
        assert "Pulando origem já carregada" not in caplog.text
        assert "old-" not in caplog.text
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
        if backend == "table":
            spark.sql(f"DROP TABLE IF EXISTS {control} PURGE")


@pytest.mark.parametrize("mode", ["merge", "insert_only"])
def test_all_consumed_origins_recorded_before_deduplication(
    spark, tables, tmp_path, mode
):
    target, control = tables
    source = write_source(
        spark, tmp_path / "source", [("a.csv", "1", "a"), ("b.csv", "1", "b")]
    )
    loader = TrinoLoader(spark)
    loader.upsert_iceberg(
        source, target, ["nu_referencia"], mode=mode, control_table=control
    )
    assert spark.table(target).count() == 1
    assert set(read_control(spark, control)) == {"a.csv", "b.csv"}
    next_source = write_source(spark, tmp_path / "next", [("c.csv", "1", "c")])
    loader.upsert_iceberg(
        next_source, target, ["nu_referencia"], mode=mode, control_table=control
    )
    assert spark.table(target).count() == 1
    assert set(read_control(spark, control)) == {"a.csv", "b.csv", "c.csv"}


@pytest.mark.parametrize("empty", [False, True])
def test_no_eligible_origins_prepares_control_without_loading_data(
    spark, tables, tmp_path, empty
):
    target, control = tables
    records = [] if empty else [("known.csv", "1", "old")]
    source = write_source(spark, tmp_path / "source", records)
    if not empty:
        write_control(
            spark.createDataFrame([("known.csv", _OLD_LOAD)], _CONTROL_SCHEMA), control
        )
    TrinoLoader(spark).upsert_iceberg(
        source, target, mode="append", control_table=control
    )
    assert not spark.catalog.tableExists(target)
    assert read_control(spark, control) == ({} if empty else {"known.csv": _OLD_LOAD})


def test_existing_records_preserved_and_reloaded_names_updated(spark, tables, tmp_path):
    target, control = tables
    seed = spark.createDataFrame(
        [("old.csv", _OLD_LOAD), ("reload.csv", _OLD_LOAD)],
        _CONTROL_SCHEMA,
    )
    write_control(seed, control)
    source = write_source(spark, tmp_path / "source", [("reload.csv", "1", "updated")])
    TrinoLoader(spark).upsert_iceberg(source, target, control_table=control)
    actual = read_control(spark, control)
    assert actual["old.csv"] == _OLD_LOAD
    assert actual["reload.csv"] > _OLD_LOAD


@pytest.mark.parametrize("invalid", [None, "", " \t\n", "\u00a0"])
def test_invalid_origin_fails_without_writes(spark, tables, tmp_path, invalid):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [(invalid, "1", "bad")])
    with pytest.raises(ValueError, match="no_arquivo_origem"):
        TrinoLoader(spark).upsert_iceberg(source, target, control_table=control)
    assert not spark.catalog.tableExists(target)
    assert read_control(spark, control) == {}


@pytest.mark.parametrize(
    "invalid",
    ["extra_column", "wrong_timestamp", "null_timestamp", "duplicate", "wrong_origin"],
)
def test_invalid_control_fails_without_writes(spark, tables, tmp_path, invalid):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    seed = spark.createDataFrame([("old.csv", _OLD_LOAD)], _CONTROL_SCHEMA)
    if invalid == "extra_column":
        seed = seed.withColumn("extra", F.lit(1))
    elif invalid == "wrong_timestamp":
        seed = seed.withColumn("dh_carga", F.col("dh_carga").cast("timestamp"))
    elif invalid == "null_timestamp":
        seed = seed.withColumn("dh_carga", F.lit(None).cast("timestamp_ntz"))
    elif invalid == "duplicate":
        seed = seed.unionByName(seed)
    else:
        seed = seed.withColumn("no_arquivo_origem", F.lit(123))
    write_control(seed, control)
    with pytest.raises((ValueError, TypeError), match="control_table"):
        TrinoLoader(spark).upsert_iceberg(source, target, control_table=control)
    assert not spark.catalog.tableExists(target)


@pytest.mark.parametrize("timezone", ["UTC", "America/Sao_Paulo", "Asia/Tokyo"])
def test_timestamp_is_utc_without_session_timezone_conversion(
    spark, tables, tmp_path, monkeypatch, timezone
):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    previous = spark.conf.get("spark.sql.session.timeZone")
    fixed = datetime.fromisoformat("2026-10-29T13:06:25.123456+00:00")

    class Clock:
        @staticmethod
        def now(tz):
            return fixed.astimezone(tz)

    monkeypatch.setattr("teleutils.core.loaders.trino_loader.datetime", Clock)
    try:
        spark.conf.set("spark.sql.session.timeZone", timezone)
        TrinoLoader(spark).upsert_iceberg(source, target, control_table=control)
        assert read_control(spark, control) == {"new.csv": fixed.replace(tzinfo=None)}
        assert spark.conf.get("spark.sql.session.timeZone") == timezone
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


def test_failure_before_or_after_data_commit(
    spark, tables, tmp_path, monkeypatch, caplog
):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    loader = TrinoLoader(spark)

    def fail(*_args, **_kwargs):
        raise RuntimeError("injected failure")

    with monkeypatch.context() as context:
        context.setattr(loader, "_write_data", fail)
        with pytest.raises(RuntimeError, match="injected"):
            loader.upsert_iceberg(source, target, control_table=control)
    assert not spark.catalog.tableExists(target)
    assert read_control(spark, control) == {}
    with monkeypatch.context() as context:
        context.setattr(loader, "_record_control", fail)
        with pytest.raises(RuntimeError, match="injected"):
            loader.upsert_iceberg(source, target, control_table=control)
    assert spark.table(target).count() == 1
    assert read_control(spark, control) == {}
    assert "Falha parcial" in caplog.text


def test_control_catalog_failure_is_not_treated_as_missing(
    spark, tables, tmp_path, monkeypatch
):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    loader = TrinoLoader(spark)

    def fail(_table):
        raise PermissionError("catalog access denied")

    with monkeypatch.context() as context:
        context.setattr(spark.catalog, "tableExists", fail)
        with pytest.raises(PermissionError, match="catalog access denied"):
            loader.upsert_iceberg(source, target, control_table=control)
    assert not spark.catalog.tableExists(target)


def test_preflight_preserves_records_before_source_read(spark, tables, tmp_path):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    loader = TrinoLoader(spark)
    write_control(
        spark.createDataFrame([("old.csv", _OLD_LOAD)], _CONTROL_SCHEMA), control
    )
    prepared = loader._prepare_control(control, source, target)
    assert prepared.count() == 1
    assert read_control(spark, control) == {"old.csv": _OLD_LOAD}
    assert not spark.catalog.tableExists(target)
    assert not list(tmp_path.glob("*.teleutils_*"))
    assert not list((tmp_path / "control.parquet").glob("_teleutils_probe_*"))


def test_internal_storage_is_released_without_uncaching_caller(spark, tables, tmp_path):
    target, control = tables
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    seed = spark.createDataFrame([], _CONTROL_SCHEMA).cache()
    seed.count()
    before = spark.sparkContext._jsc.getPersistentRDDs().size()
    try:
        TrinoLoader(spark).upsert_iceberg(source, target, control_table=control)
        assert seed.is_cached
        assert spark.sparkContext._jsc.getPersistentRDDs().size() == before
    finally:
        seed.unpersist()


def test_control_path_cannot_overlap_iceberg_data(spark, tmp_path):
    target = f"test.loads.overlap_{uuid4().hex}"
    source = write_source(spark, tmp_path / "source", [("new.csv", "1", "new")])
    try:
        TrinoLoader(spark).upsert_iceberg(source, target)
        details = spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
        location = next(row.data_type for row in details if row.col_name == "Location")
        control = Path(location.removeprefix("file:")).as_uri()
        with pytest.raises(ValueError, match="sobrepor"):
            TrinoLoader(spark).upsert_iceberg(
                source, target, mode="append", control_table=control
            )
        assert spark.table(target).count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
