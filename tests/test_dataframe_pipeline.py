import sys
from collections import Counter

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from teleutils._config import MIN_SAFE_DATE
from teleutils.core.extractors import CDRParquetExtractor, CDRTextExtractor
from teleutils.core.extractors.schemas import CDRParquetSchema, CDRTextSchema
from teleutils.core.transformers import CDRTransformer


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .appName("teleutils-dataframe-pipeline-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


def test_same_session_frame_is_returned_without_execution(spark, monkeypatch):
    transformer = CDRTransformer(spark)
    frame = transformer.spark.range(1)

    def reject_action(*args, **kwargs):
        pytest.fail("Resolver a entrada não deve executar o DataFrame")

    monkeypatch.setattr(type(frame), "collect", reject_action)
    monkeypatch.setattr(type(frame), "count", reject_action)
    assert transformer._resolve_input(frame) is frame
    projected = frame.select("id")
    assert transformer._resolve_input(projected) is projected


@pytest.mark.parametrize("source_session", ["caller", "sibling"])
def test_wrong_session_is_rejected_before_writing(spark, tmp_path, source_session):
    transformer = CDRTransformer(spark)
    other = spark if source_session == "caller" else spark.newSession()
    if source_session == "sibling":
        other.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    target = tmp_path / "output"
    with pytest.raises(ValueError, match="transformer.spark"):
        transformer.transform_stfc_huawei_ngn_tim(other.range(1), str(target))
    assert not target.exists()
    assert spark.range(1).count() == 1


@pytest.mark.parametrize("value", [None, 1, [], object()])
def test_invalid_transformer_input_is_explicit(spark, value):
    with pytest.raises(TypeError, match="source_file"):
        CDRTransformer(spark)._resolve_input(value)


def test_timestamp_configuration_drift_fails_at_input_and_write(spark, tmp_path):
    transformer = CDRTransformer(spark)
    frame = transformer.spark.range(1)
    transformer.spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
    with pytest.raises(ValueError, match="TIMESTAMP_NTZ"):
        transformer._resolve_input(frame)
    with pytest.raises(ValueError, match="TIMESTAMP_NTZ"):
        transformer._write_parquet(frame, str(tmp_path / "output"))
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("caller_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_caller_runtime_overrides_are_not_copied_or_changed(spark, caller_type):
    keys = (
        "spark.sql.timestampType",
        "spark.sql.session.timeZone",
        "spark.sql.ansi.enabled",
    )
    before = {key: spark.conf.get(key) for key in keys}
    defaults = spark.newSession()
    try:
        spark.conf.set(keys[0], caller_type)
        spark.conf.set(keys[1], "America/New_York")
        spark.conf.set(keys[2], "true")
        transformer = CDRTransformer(spark)
        assert transformer.spark.conf.get(keys[0]) == "TIMESTAMP_NTZ"
        for key in keys[1:]:
            assert transformer.spark.conf.get(key) == defaults.conf.get(key)
        assert [spark.conf.get(key) for key in keys] == [
            caller_type,
            "America/New_York",
            "true",
        ]
        assert transformer.spark.sparkContext is spark.sparkContext
    finally:
        for key, value in before.items():
            spark.conf.set(key, value)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_timestamp_normalization_stays_ntz_and_tolerates_invalid_values(spark, ansi):
    transformer = CDRTransformer(spark)
    transformer.spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    transformer.spark.conf.set("spark.sql.ansi.enabled", ansi)
    frame = transformer.spark.createDataFrame(
        [
            ("gap", "2024-03-10 02:30:00", None, "60"),
            ("overlap", "2024-11-03 01:30:00", "2024-11-03 01:31:00", "60"),
            ("invalid", "invalid", "invalid", "60"),
            ("null", None, None, "0"),
            ("early", "0001-01-01 00:00:00", None, "0"),
        ],
        "id string, data_hora string, data_hora_fim string, duracao string",
    ).withColumn("data_hora_referencia", F.col("data_hora"))
    result = transformer._format_date_time(frame, "yyyy-MM-dd HH:mm:ss")
    for name in ("data_hora", "data_hora_fim", "data_hora_referencia"):
        assert result.schema[name].dataType == T.TimestampNTZType()
    actual = {
        row.id: (row.start, row.end)
        for row in result.select(
            "id",
            F.col("data_hora").cast("string").alias("start"),
            F.col("data_hora_fim").cast("string").alias("end"),
        ).collect()
    }
    minimum = (
        transformer.spark.range(1)
        .select(F.lit(MIN_SAFE_DATE).cast("timestamp_ntz").cast("string"))
        .first()[0]
    )
    assert actual["gap"] == ("2024-03-10 02:30:00", "2024-03-10 02:31:00")
    assert actual["overlap"] == ("2024-11-03 01:30:00", "2024-11-03 01:31:00")
    for key in ("invalid", "null", "early"):
        assert actual[key] == (minimum, minimum)


LAYOUT_METHODS = [
    name for name in vars(CDRTransformer) if name.startswith("transform_")
]


@pytest.mark.parametrize("method_name", LAYOUT_METHODS)
def test_every_layout_uses_shared_input_resolver(spark, monkeypatch, method_name):
    transformer = CDRTransformer(spark)
    frame = transformer.spark.range(1)

    def resolve(source):
        assert source is frame
        raise RuntimeError("resolver reached")

    monkeypatch.setattr(transformer, "_resolve_input", resolve)
    with pytest.raises(RuntimeError, match="resolver reached"):
        getattr(transformer, method_name)(source_file=frame, target_file="unused")


@pytest.mark.parametrize("unique", [True, False])
def test_parquet_dataframe_preserves_merge_missing_columns_and_duplicates(
    spark, tmp_path, unique
):
    source = tmp_path / "source"
    spark.createDataFrame(
        [("same", "a"), ("same", "a")], "value string, extra string"
    ).coalesce(1).write.parquet(str(source / "first"))
    spark.createDataFrame([("other",)], "value string").write.parquet(
        str(source / "second")
    )
    schema = CDRParquetSchema(
        "custom",
        (("value", "payload"), ("extra", "extra"), ("absent", "missing")),
        "test",
    )
    transformer = CDRTransformer(spark)
    transformer.spark.conf.set("spark.sql.shuffle.partitions", "2")
    extractor = CDRParquetExtractor(transformer.spark, {"custom": schema})
    sources = [str(source / "first"), str(source / "second")]
    arguments = {"operator": "op", "service": "voz", "cdr_type": "cdr"}
    frame = extractor.extract_dataframe(sources, "custom", unique, **arguments)
    assert frame.sparkSession is transformer.spark
    output = str(tmp_path / "extracted")
    extractor.extract(sources, output, "custom", unique, **arguments)
    rows = frame.collect()
    assert Counter(rows) == Counter(transformer.spark.read.parquet(output).collect())
    assert len(rows) == (2 if unique else 3)
    assert all(row.missing is None for row in rows)
    assert next(row for row in rows if row.payload == "other").extra is None


@pytest.mark.parametrize("read_rdd", [True, False])
def test_fixed_width_dataframe_filter_matches_materialized(spark, tmp_path, read_rdd):
    source = tmp_path / "records.txt"
    source.write_text("5hello\n0other\n5again\n")
    schema = CDRTextSchema(
        "custom",
        "|",
        "value string",
        False,
        ("kind", "5"),
        (1, 2),
        (1, 5),
        ("kind", "payload"),
        "test",
        read_rdd=read_rdd,
    )
    transformer = CDRTransformer(spark)
    extractor = CDRTextExtractor(transformer.spark, {"custom": schema})
    arguments = {"operator": "op", "service": "voz", "cdr_type": "cdr"}
    frame = extractor.extract_dataframe(str(source), "custom", **arguments)
    assert frame.sparkSession is transformer.spark
    target = str(tmp_path / "extracted")
    extractor.extract(str(source), target, "custom", **arguments)
    assert Counter(frame.collect()) == Counter(
        transformer.spark.read.parquet(target).collect()
    )
    assert {row.payload for row in frame.collect()} == {"hello", "again"}
