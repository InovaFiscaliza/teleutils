import os
import sys
from collections import Counter
from uuid import uuid4

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.readwriter import DataFrameWriterV2

from teleutils._config import TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader

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


@pytest.fixture
def target(spark):
    table = f"test.loads.data_{uuid4().hex}"
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def write_source(spark, path, records, *, reference_type=None):
    schema = T.StructType(
        [
            T.StructField(
                name,
                reference_type
                if name == "nu_referencia" and reference_type is not None
                else kind,
            )
            for name, kind in TARGET_SCHEMA.values()
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


def rows(spark, target):
    return Counter(
        tuple(row)
        for row in spark.table(target)
        .select("no_arquivo_origem", "nu_referencia", "no_bilhetador")
        .collect()
    )


def test_new_origins_only_append_to_existing_iceberg(spark, target, tmp_path):
    loader = TrinoLoader(spark)
    first = ("a.csv", "1", "first")
    new = ("b.csv", "1", "new")
    loader.upsert_iceberg(write_source(spark, tmp_path / "first", [first]), target)
    before = spark.table(f"{target}.snapshots").count()
    loader.upsert_iceberg(write_source(spark, tmp_path / "new", [new]), target)
    assert rows(spark, target) == Counter([first, new])
    assert spark.table(f"{target}.snapshots").count() == before + 1


def test_reload_multiple_origins_replaces_only_their_rows(spark, target, tmp_path):
    loader = TrinoLoader(spark)
    keep = ("a'`.csv.backup", "1", "keep")
    initial = [("a'`.csv", "1", "old"), ("b.csv", "2", "old"), keep]
    loader.upsert_iceberg(write_source(spark, tmp_path / "first", initial), target)
    replacement = [
        ("a'`.csv", "3", "updated"),
        ("a'`.csv", "3", "updated"),
        ("new.csv", "1", "new"),
        ("b.csv", "2", "updated"),
    ]
    source = write_source(spark, tmp_path / "replacement", replacement)
    for _ in range(2):
        loader.upsert_iceberg(source, target)
        assert rows(spark, target) == Counter([keep, *replacement])


def test_empty_parquet_does_not_create_destination(spark, target, tmp_path):
    source = write_source(spark, tmp_path / "empty", [])
    TrinoLoader(spark).upsert_iceberg(source, target)
    assert not spark.catalog.tableExists(target)


def test_incompatible_source_preserves_existing_rows(spark, target, tmp_path):
    loader = TrinoLoader(spark)
    initial = ("a.csv", "1", "old")
    loader.upsert_iceberg(write_source(spark, tmp_path / "first", [initial]), target)
    source = write_source(
        spark,
        tmp_path / "bad",
        [("a.csv", ["1"], "new")],
        reference_type=T.ArrayType(T.StringType()),
    )
    with pytest.raises(AnalysisException, match="nu_referencia"):
        loader.upsert_iceberg(source, target)
    assert rows(spark, target) == Counter([initial])


def test_insert_failure_after_delete_can_be_reprocessed(
    spark, target, tmp_path, monkeypatch, caplog
):
    loader = TrinoLoader(spark)
    keep = ("b.csv", "2", "keep")
    initial = [("a.csv", "1", "old"), keep]
    loader.upsert_iceberg(write_source(spark, tmp_path / "first", initial), target)
    replacement = ("a.csv", "3", "updated")
    source = write_source(spark, tmp_path / "new", [replacement])

    def fail_insert(writer):
        raise RuntimeError("insert failed")

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriterV2, "append", fail_insert)
        with pytest.raises(RuntimeError, match="insert failed"):
            loader.upsert_iceberg(source, target)
    assert "Falha parcial" in caplog.text
    assert rows(spark, target) == Counter([keep])

    loader.upsert_iceberg(source, target)
    assert rows(spark, target) == Counter([keep, replacement])
