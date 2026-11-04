import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.readwriter import DataFrameWriter

from teleutils._config import TARGET_SCHEMA
from teleutils.core.loaders import TrinoLoader
from teleutils.core.loaders.trino_loader import _CONTROL_SCHEMA

_OLD_LOAD = datetime(2020, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.Builder()
            .master("local[2]")
            .appName("teleutils-control-parquet-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


def test_missing_path_created_and_probe_removed(spark, tmp_path):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control.parquet").as_uri()
    result = loader._prepare_control(control, str(tmp_path / "source"), "unused_target")
    assert result.isEmpty()
    assert result.schema.simpleString() == _CONTROL_SCHEMA.simpleString()
    assert not list(tmp_path.glob("*.teleutils_*"))
    assert not list((tmp_path / "control.parquet").glob("_teleutils_probe_*"))


def test_preflight_leaves_records_unchanged_and_overwrite_preserves_unaffected_origins(
    spark, tmp_path
):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control.parquet").as_uri()
    initial = spark.createDataFrame(
        [("old.csv", _OLD_LOAD), ("reload.csv", _OLD_LOAD)], _CONTROL_SCHEMA
    )
    initial.write.parquet(control)
    for _ in range(2):
        prepared = loader._prepare_control(
            control, str(tmp_path / "source"), "unused_target"
        )
        if _ == 0:
            assert sorted(prepared.collect()) == sorted(initial.collect())
        origins = spark.createDataFrame(
            [("new.csv",), ("reload.csv",)], ["no_arquivo_origem"]
        )
        loader._record_control(origins, control, prepared)
        rows = {
            row.no_arquivo_origem: row.dh_carga
            for row in spark.read.parquet(control).collect()
        }
        assert set(rows) == {"old.csv", "new.csv", "reload.csv"}
        assert rows["old.csv"] == _OLD_LOAD
        assert rows["reload.csv"] > _OLD_LOAD
    assert not list(tmp_path.glob("*.teleutils_*"))


@pytest.mark.parametrize("kind", ["file", "empty_directory", "bad_schema"])
def test_existing_invalid_path_is_not_overwritten(spark, tmp_path, kind):
    path = tmp_path / "control"
    if kind == "file":
        path.write_text("keep me", encoding="utf-8")
    elif kind == "empty_directory":
        path.mkdir()
    else:
        spark.range(1).write.parquet(str(path))
    loader = TrinoLoader(spark)
    with pytest.raises((ValueError, AnalysisException)):
        loader._prepare_control(
            path.as_uri(), str(tmp_path / "source"), "unused_target"
        )
    if kind == "file":
        assert path.read_text(encoding="utf-8") == "keep me"
    elif kind == "empty_directory":
        assert list(path.iterdir()) == []
    else:
        assert spark.read.parquet(str(path)).columns == ["id"]


@pytest.mark.parametrize(
    "relation", ["same", "ancestor", "descendant", "glob", "symlink"]
)
def test_control_must_not_overlap_source(spark, tmp_path, relation):
    source = tmp_path / "source"
    source.mkdir()
    if relation == "same":
        control = source
    elif relation == "ancestor":
        control = tmp_path
    elif relation == "descendant":
        control = source / "control"
    elif relation == "glob":
        control = source / "control"
        source = source / "*.parquet"
    else:
        control = tmp_path / "link"
        control.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError, match="sobrepor"):
        TrinoLoader(spark)._prepare_control(
            control.as_uri(), str(source), "unused_target"
        )


@pytest.mark.parametrize("phase", ["create", "inside_probe", "sibling_probe"])
def test_write_probe_failure_prevents_source_read(
    spark, tmp_path, monkeypatch, caplog, phase
):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control").as_uri()
    original = DataFrameWriter.parquet
    write_data = MagicMock()
    monkeypatch.setattr(loader, "_write_data", write_data)

    def fail_probe(writer, path, *args, **kwargs):
        if (
            (phase == "create" and path == control)
            or (phase == "inside_probe" and "_teleutils_probe_" in path)
            or (phase == "sibling_probe" and ".teleutils_" in path)
        ):
            raise PermissionError("write denied before loading")
        return original(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_probe)
    with pytest.raises(PermissionError, match="write denied"):
        loader.upsert_iceberg(
            str(tmp_path / "missing_source"), "unused_target", control_table=control
        )
    write_data.assert_not_called()
    assert "Lendo Parquet para carga" not in caplog.text
    assert not list(tmp_path.glob("*.teleutils_*"))


def test_cleanup_failure_prevents_loading(spark, tmp_path, monkeypatch):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control").as_uri()
    original_path = loader._hadoop_path
    write_data = MagicMock()
    monkeypatch.setattr(loader, "_write_data", write_data)

    def filesystem_with_failed_delete(value):
        path, filesystem = original_path(value)
        if "_teleutils_probe_" in value:
            proxy = MagicMock(wraps=filesystem)
            proxy.delete.return_value = False
            return path, proxy
        return path, filesystem

    monkeypatch.setattr(loader, "_hadoop_path", filesystem_with_failed_delete)
    with pytest.raises(OSError, match="remover temporário"):
        loader.upsert_iceberg(
            str(tmp_path / "missing_source"), "unused_target", control_table=control
        )
    write_data.assert_not_called()


def test_overwrite_failure_is_propagated_without_rollback(spark, tmp_path, monkeypatch):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control").as_uri()
    prepared = loader._prepare_control(
        control, str(tmp_path / "source"), "unused_target"
    )
    original = DataFrameWriter.parquet
    origins = spark.createDataFrame([("new.csv",)], ["no_arquivo_origem"])

    def fail_final_write(writer, path, *args, **kwargs):
        if path == control:
            raise OSError("overwrite failed")
        return original(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_final_write)
    with pytest.raises(OSError, match="overwrite failed"):
        loader._record_control(origins, control, prepared)
    assert not list(tmp_path.glob("*.teleutils_*"))


def test_encoded_uri_uses_literal_filesystem_name(spark, tmp_path):
    loader = TrinoLoader(spark)
    path = tmp_path / "controle + # á.parquet"
    resolved = loader._resolve_control(["unused_target"], path.as_uri())
    assert resolved is not None
    prepared = loader._prepare_control(
        resolved, str(tmp_path / "source"), "unused_target"
    )
    assert prepared.isEmpty()
    assert path.is_dir()
    assert not list(tmp_path.glob("*.teleutils_*"))


def test_symlink_to_filesystem_root_is_rejected(spark, tmp_path):
    link = tmp_path / "root_link"
    link.symlink_to("/", target_is_directory=True)
    with pytest.raises(ValueError, match="raiz"):
        TrinoLoader(spark)._prepare_control(
            link.as_uri(), "s3a://bucket/source", "unused_target"
        )


def test_actual_overwrite_error_after_data_load_is_logged_as_partial_failure(
    spark, tmp_path, monkeypatch, caplog
):
    loader = TrinoLoader(spark)
    control = (tmp_path / "control").as_uri()
    source = str(tmp_path / "source")
    schema = T.StructType(
        [
            T.StructField(name, data_type, True)
            for name, data_type in TARGET_SCHEMA.values()
        ]
    )
    spark.createDataFrame([{"no_arquivo_origem": "new.csv"}], schema).write.parquet(
        source
    )
    loader._prepare_control(control, source, "unused_target")
    original = DataFrameWriter.parquet
    write_data = MagicMock()
    monkeypatch.setattr(loader, "_write_data", write_data)

    def fail_overwrite(writer, path, *args, **kwargs):
        if path == control:
            raise OSError("final control write failed")
        return original(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_overwrite)
    with pytest.raises(OSError, match="final control write failed"):
        loader.upsert_iceberg(source, "unused_target", control_table=control)
    write_data.assert_called_once()
    assert "Falha parcial" in caplog.text
    assert not list(tmp_path.glob("*.teleutils_*"))
