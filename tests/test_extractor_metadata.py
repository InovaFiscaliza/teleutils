from __future__ import annotations

import logging
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from teleutils.core.extractors._extractors_utils import (
    SOURCE_PATH_COLUMN,
    add_source_metadata,
    read_text_with_source,
    validate_metadata_arguments,
)


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .appName("teleutils-extractor-metadata-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


@pytest.mark.parametrize("name", ["operator", "service", "cdr_type"])
@pytest.mark.parametrize("value,error_type", [(1, TypeError), ("  ", ValueError)])
def test_metadata_arguments_reject_invalid_values(name, value, error_type):
    arguments = {"operator": "", "service": "", "cdr_type": ""}
    arguments[name] = value
    with pytest.raises(error_type):
        validate_metadata_arguments(**arguments)


def test_metadata_arguments_accept_custom_names():
    validate_metadata_arguments("nova prestadora", "novo servico", "tipo personalizado")
    validate_metadata_arguments(None, "", None)


@pytest.mark.parametrize(
    "path",
    [
        "file:///base/op/voz/cdr/arquivo%20%2B@.csv",
        "hdfs://host:8020/base/op/voz/cdr/arquivo%20%2B@.csv",
        "/base/op/voz/cdr/arquivo%20%2B@.csv",
    ],
)
def test_metadata_infers_directories_and_decodes_filename(spark, path):
    df = spark.createDataFrame([(path,)], [SOURCE_PATH_COLUMN])
    result = add_source_metadata(df, "custom").first().asDict()
    assert result == {
        "prestadora": "op",
        "servico": "voz",
        "tipo_cdr": "cdr",
        "esquema": "custom",
        "arquivo_origem": "arquivo +@.csv",
    }


def test_metadata_arguments_override_short_path(spark, monkeypatch):
    df = spark.createDataFrame([("/arquivo+.csv",)], [SOURCE_PATH_COLUMN])
    with monkeypatch.context() as checks:

        def unexpected_collect(*args, **kwargs):
            raise AssertionError("Argumentos completos não devem validar diretórios.")

        checks.setattr(type(df), "collect", unexpected_collect)
        result_df = add_source_metadata(
            df, "custom", operator="op", service="voz", cdr_type="cdr"
        )
    result = result_df.first()
    assert (result.prestadora, result.servico, result.tipo_cdr) == ("op", "voz", "cdr")
    assert result.arquivo_origem == "arquivo+.csv"


def test_metadata_only_infers_missing_fields(spark):
    df = spark.createDataFrame([("/voz/cdr/arquivo.csv",)], [SOURCE_PATH_COLUMN])
    result = add_source_metadata(df, "custom", operator="op").first()
    assert (result.prestadora, result.servico, result.tipo_cdr) == ("op", "voz", "cdr")


def test_metadata_rejects_missing_directory_without_uri_authority_fallback(spark):
    df = spark.createDataFrame(
        [("hdfs://host/voz/cdr/arquivo.csv",)], [SOURCE_PATH_COLUMN]
    )
    with pytest.raises(ValueError, match="prestadora"):
        add_source_metadata(df, "custom")


def test_content_type_overrides_argument_and_skips_type_inference(spark, caplog):
    df = spark.createDataFrame(
        [
            ("/arquivo.csv", "aTSRecord"),
            ("/arquivo.csv", "iBCFRecord"),
            ("/arquivo.csv", None),
        ],
        f"{SOURCE_PATH_COLUMN} string, _tipo_cdr string",
    )
    with caplog.at_level(logging.INFO):
        result = add_source_metadata(
            df, "custom", operator="op", service="voz", cdr_type="ignored"
        )
    assert [row.tipo_cdr for row in result.collect()] == [
        "aTSRecord",
        "iBCFRecord",
        None,
    ]
    assert "_tipo_cdr encontrada" in caplog.text
    assert "ignorado" in caplog.text
    assert SOURCE_PATH_COLUMN not in result.columns


@pytest.mark.parametrize("source_mode", ["file", "directory", "glob", "list"])
def test_rdd_reader_preserves_each_filename(spark, tmp_path, source_mode):
    folder = tmp_path / "op" / "voz" / "cdr"
    folder.mkdir(parents=True)
    first = folder / "primeiro@+%.txt"
    second = folder / "segundo@.txt"
    first.write_text("primeiro\n", encoding="utf-8")
    second.write_text("segundo\n", encoding="utf-8")
    (folder / "_SUCCESS").write_text("ignorado\n", encoding="utf-8")
    sources = {
        "file": str(first),
        "directory": str(folder),
        "glob": str(folder / "*.txt"),
        "list": [str(first), str(second)],
    }
    result = add_source_metadata(
        read_text_with_source(spark, sources[source_mode]), "custom"
    )
    actual = {row.value: row.arquivo_origem for row in result.collect()}
    expected = {"primeiro": first.name}
    if source_mode != "file":
        expected["segundo"] = second.name
    assert actual == expected


@pytest.mark.parametrize("format_name", ["csv", "rdd", "parquet"])
@pytest.mark.parametrize("metadata_mode", ["inferred", "explicit", "content"])
@pytest.mark.parametrize("materialized", [True, False])
def test_extractors_share_metadata_contract(
    spark, tmp_path, format_name, metadata_mode, materialized, caplog, monkeypatch
):
    from teleutils.core.extractors.parquet_extractors import CDRParquetExtractor
    from teleutils.core.extractors.schemas import CDRParquetSchema, CDRTextSchema
    from teleutils.core.extractors.text_extractors import CDRTextExtractor

    folder = tmp_path / "op" / "voz" / "cdr"
    folder.parent.mkdir(parents=True)
    target = str(tmp_path / "output")
    has_content_type = metadata_mode == "content"
    if format_name == "parquet":
        values = [("hello", "aTSRecord")] if has_content_type else [("hello",)]
        fields = (
            "value string, record_type string" if has_content_type else "value string"
        )
        spark.createDataFrame(values, fields).write.parquet(str(folder))
        mapping = (("value", "payload"),)
        if has_content_type:
            mapping += (("record_type", "_tipo_cdr"),)
        schema = CDRParquetSchema("custom", mapping, "test")
        extractor = CDRParquetExtractor(spark, {"custom": schema})
        source = str(folder)
    else:
        folder.mkdir()
        path = folder / "entrada@+%.txt"
        content = "hello"
        indices = (1,) if format_name == "rdd" else (0,)
        sizes = (5,) if format_name == "rdd" else ()
        names = ("payload",)
        if has_content_type:
            content += "aTSRecord" if format_name == "rdd" else ",aTSRecord"
            indices += (6,) if format_name == "rdd" else (1,)
            sizes += (9,) if format_name == "rdd" else ()
            names += ("_tipo_cdr",)
        path.write_text(content + "\n", encoding="utf-8")
        schema = CDRTextSchema(
            name="custom",
            delimiter=",",
            schema=None,
            has_header=False,
            lines_to_keep=None,
            column_indices=indices,
            column_sizes=sizes,
            column_names=names,
            job_description="test",
            read_rdd=format_name == "rdd",
        )
        extractor = CDRTextExtractor(spark, {"custom": schema})
        source = str(path)
    arguments = {}
    expected = ("op", "voz", "cdr", "custom")
    if metadata_mode != "inferred":
        arguments = {
            "operator": "explicit",
            "service": "custom service",
            "cdr_type": "custom type",
        }
        expected = ("explicit", "custom service", "custom type", "custom")
    if has_content_type:
        expected = (expected[0], expected[1], "aTSRecord", "custom")
    with caplog.at_level(logging.INFO):
        if materialized:
            assert extractor.extract(source, target, "custom", **arguments) == target
            frame = spark.read.parquet(target)
        else:
            from pyspark.sql.readwriter import DataFrameWriter

            def reject_write(*args, **kwargs):
                pytest.fail("extract_dataframe não pode gravar Parquet")

            monkeypatch.setattr(DataFrameWriter, "parquet", reject_write)
            frame = extractor.extract_dataframe(source, "custom", **arguments)
            assert frame.sparkSession is spark
    result = frame.first()
    assert result.payload == "hello"
    assert (
        result.prestadora,
        result.servico,
        result.tipo_cdr,
        result.esquema,
    ) == expected
    assert "_tipo_cdr" not in result.asDict()
    if has_content_type:
        assert "_tipo_cdr encontrada" in caplog.text
    assert SOURCE_PATH_COLUMN not in result.asDict()
    if format_name == "parquet":
        assert result.arquivo_origem.startswith("part-")
    else:
        assert result.arquivo_origem == path.name


def test_missing_inferred_metadata_fails_before_writing(spark, tmp_path, monkeypatch):
    from teleutils.core.extractors.schemas import CDRTextSchema
    from teleutils.core.extractors.text_extractors import CDRTextExtractor

    source = tmp_path / "entrada.csv"
    source.write_text("hello\n", encoding="utf-8")
    schema = CDRTextSchema(
        "custom", ",", None, False, None, (0,), (), ("payload",), "test"
    )
    monkeypatch.setattr(F, "input_file_name", lambda: F.lit("file:///arquivo.csv"))
    target = tmp_path / "output"
    with pytest.raises(ValueError, match="prestadora"):
        CDRTextExtractor(spark, {"custom": schema}).extract(
            str(source), str(target), "custom"
        )
    assert not target.exists()
