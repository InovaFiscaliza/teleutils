import logging
import sys
from unittest.mock import Mock

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from teleutils.core.extractors import (
    CDRParquetExtractor,
    CDRTextExtractor,
    text_extractors,
)
from teleutils.core.orchestrators import run_etl_item


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .appName("teleutils-missing-input-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


@pytest.mark.parametrize("kind", ["csv", "rdd", "parquet"])
@pytest.mark.parametrize("source_kind", ["missing", "glob", "list"])
@pytest.mark.parametrize("method", ["extract", "extract_dataframe"])
def test_missing_input_warns_once_and_preserves_output(
    spark, tmp_path, caplog, kind, source_kind, method
):
    if kind == "parquet":
        extractor = CDRParquetExtractor(spark)
        schema = "stfc_italtel_ngn_tim"
        existing = tmp_path / "existing.parquet"
        spark.createDataFrame([("value",)], "value string").write.parquet(str(existing))
    else:
        extractor = CDRTextExtractor(spark)
        schema = "stfc_7n_oi" if kind == "rdd" else "stfc_huawei_ngn_tim"
        existing = tmp_path / "existing.txt"
        existing.write_text("value\n", encoding="utf-8")

    missing = tmp_path / ("missing-*.txt" if source_kind == "glob" else "missing.txt")
    source = [str(existing), str(missing)] if source_kind == "list" else str(missing)
    target = tmp_path / "output"
    target.mkdir()
    marker = target / "untouched.txt"
    marker.write_text("preserve", encoding="utf-8")
    arguments = {"operator": "op", "service": "voz", "cdr_type": "cdr"}

    with caplog.at_level(logging.INFO):
        if method == "extract":
            result = extractor.extract(source, str(target), schema, **arguments)
        else:
            result = extractor.extract_dataframe(source, schema, **arguments)

    assert result is None
    assert marker.read_text(encoding="utf-8") == "preserve"
    records = [
        record
        for record in caplog.records
        if record.name == extractor.__class__.__module__
    ]
    warnings = [record for record in records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "missing" in warnings[0].getMessage()
    assert not any(record.exc_info for record in records)
    assert not any(
        "concluída com sucesso" in record.getMessage()
        or "preparado, sem persistência" in record.getMessage()
        for record in records
    )


@pytest.mark.parametrize("kind", ["csv", "parquet"])
def test_spark_missing_path_is_a_warning_without_exception(kind, caplog):
    error = AnalysisException(message="missing input")
    error.error_class = "PATH_NOT_FOUND"
    spark = Mock()
    if kind == "csv":
        extractor = CDRTextExtractor(spark)
        schema = "stfc_huawei_ngn_tim"
        spark.read.csv.side_effect = error
    else:
        extractor = CDRParquetExtractor(spark)
        schema = "stfc_italtel_ngn_tim"
        spark.read.option.return_value.option.return_value.parquet.side_effect = error

    with caplog.at_level(logging.INFO):
        assert extractor.extract_dataframe("missing-input", schema) is None

    warnings = [
        record
        for record in caplog.records
        if record.name == extractor.__class__.__module__
        and record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "missing-input" in warnings[0].getMessage()
    assert warnings[0].exc_info is None
    assert "preparado, sem persistência" not in caplog.text


@pytest.mark.parametrize("kind", ["csv", "parquet"])
def test_other_analysis_exception_is_propagated(kind, caplog):
    error = AnalysisException(message="outra falha")
    spark = Mock()
    if kind == "csv":
        extractor = CDRTextExtractor(spark)
        schema = "stfc_huawei_ngn_tim"
        spark.read.csv.side_effect = error
    else:
        extractor = CDRParquetExtractor(spark)
        schema = "stfc_italtel_ngn_tim"
        spark.read.option.return_value.option.return_value.parquet.side_effect = error

    with caplog.at_level(logging.INFO), pytest.raises(AnalysisException) as captured:
        extractor.extract_dataframe("input", schema)

    assert captured.value is error
    assert any(
        record.exc_info and record.exc_info[1] is error for record in caplog.records
    )


def test_other_rdd_file_error_is_propagated(monkeypatch, caplog):
    error = FileNotFoundError("outra falha do filesystem")

    def fail(*args):
        raise error

    monkeypatch.setattr(text_extractors, "read_text_with_source", fail)
    extractor = CDRTextExtractor(Mock())
    with caplog.at_level(logging.INFO), pytest.raises(FileNotFoundError) as captured:
        extractor.extract_dataframe("input", "stfc_7n_oi")

    assert captured.value is error
    assert any(
        record.exc_info and record.exc_info[1] is error for record in caplog.records
    )


@pytest.mark.parametrize("kind", ["csv", "parquet"])
@pytest.mark.parametrize("materialize_intermediate", [True, False])
def test_missing_input_is_reported_by_real_etl(
    spark, tmp_path, caplog, kind, materialize_intermediate
):
    schema = "stfc_huawei_ngn_tim" if kind == "csv" else "stfc_italtel_ngn_tim"
    config = {
        "storage_type": "local",
        "source_folder": str(tmp_path / "input"),
        "pattern": "missing-*",
        "schema": schema,
        "extracted_folder": str(tmp_path / "extracted"),
        "transformed_folder": str(tmp_path / "transformed"),
        "output_file_prefix": "test",
        "operator": "op",
        "service": "voz",
        "cdr_type": "cdr",
    }
    intermediate = tmp_path / "extracted" / "test_extracted.parquet"
    output = tmp_path / "transformed" / "test_transformed.parquet"
    for path in (intermediate, output):
        path.mkdir(parents=True)
        (path / "untouched.txt").write_text("preserve", encoding="utf-8")

    with caplog.at_level(logging.INFO):
        result = run_etl_item(
            config, spark=spark, materialize_intermediate=materialize_intermediate
        )

    assert (result.status, result.stage, result.completed_stages) == (
        "execution_error",
        "extraction",
        (),
    )
    assert result.error_type == "InputNotFound"
    assert result.paths.source_file in result.error_message
    for path in (intermediate, output):
        assert (path / "untouched.txt").read_text(encoding="utf-8") == "preserve"
    assert not any(record.exc_info for record in caplog.records)
    warnings = [
        record
        for record in caplog.records
        if record.name.startswith("teleutils.core.extractors.")
        and record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "missing-*" in warnings[0].getMessage()
