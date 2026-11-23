import sys
from unittest.mock import MagicMock

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from teleutils._config import TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.Builder()
            .master("local[2]")
            .appName("teleutils-loader-parquet-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


@pytest.mark.parametrize("origin", [None, "", " \t\u00a0", 123])
def test_parquet_with_invalid_origin_is_rejected_before_target_access(
    spark, tmp_path, monkeypatch, origin
):
    schema = T.StructType(
        [
            T.StructField(
                name,
                T.IntegerType()
                if name == "no_arquivo_origem" and origin == 123
                else kind,
            )
            for name, kind in TARGET_SCHEMA.values()
        ]
    )
    source = str(tmp_path / "source")
    spark.createDataFrame([{"no_arquivo_origem": origin}], schema).write.parquet(source)
    exists = MagicMock()
    monkeypatch.setattr(spark.catalog, "tableExists", exists)

    error = TypeError if origin == 123 else ValueError
    with pytest.raises(error, match="Parquet: no_arquivo_origem"):
        TrinoLoader(spark).upsert_iceberg(source, "unused_target")
    exists.assert_not_called()


def test_parquet_read_failure_is_propagated(spark, tmp_path, caplog):
    with pytest.raises(AnalysisException, match="PATH_NOT_FOUND"):
        TrinoLoader(spark).upsert_iceberg(str(tmp_path / "missing"), "unused_target")
    assert "Falha na operação [TrinoLoader.upsert_iceberg]" in caplog.text
