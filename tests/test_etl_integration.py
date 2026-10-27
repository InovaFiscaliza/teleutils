import sys
from collections import Counter
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from teleutils._config import TARGET_SCHEMA
from teleutils.core.extractors import CDRParquetExtractor, CDRTextExtractor
from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    TEXT_DEFAULT_SCHEMAS,
)
from teleutils.core.orchestrators import etl, run_etl_item


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .appName("teleutils-etl-local-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()


def local_config(tmp_path, source, schema):
    return {
        "storage_type": "local",
        "source_folder": str(source.parent),
        "pattern": source.name,
        "schema": schema,
        "extracted_folder": str(tmp_path / "extracted"),
        "transformed_folder": str(tmp_path / "transformed"),
        "output_file_prefix": "sample",
        "operator": "synthetic",
        "service": "stfc",
        "cdr_type": "test",
    }


def write_text(source, schema_name, values):
    schema = TEXT_DEFAULT_SCHEMAS[schema_name]
    fields = [""] * (max(schema.column_indices) + 1)
    for index, name in zip(schema.column_indices, schema.column_names):
        fields[index] = values.get(name, "")
    source.parent.mkdir(parents=True)
    source.write_text(schema.delimiter.join(fields) + "\n", encoding="utf-8")


@pytest.mark.parametrize("kind", ["text", "parquet"])
@pytest.mark.parametrize("directory", ["input", "espaço +@#%25"])
def test_real_pipeline_matches_direct_calls(spark, tmp_path, kind, directory):
    from teleutils.core.transformers import CDRTransformer

    values = {
        "referencia": "synthetic-1",
        "numero_origem": "11987654321",
        "numero_destino": "21987654321",
        "duracao": "60",
        "rota_entrada": "in",
        "rota_saida": "out",
        "bilhetador": "synthetic",
        "tipo_chamada": "tandem",
        "resultado_chamada": "answered",
        "data_hora": "261021120000",
        "data_hora_fim": "261021120100",
        "_data": "21/10/26",
        "_hora": "12:00:00",
        "data_hora_referencia": "2026-10-21 12:00:00",
    }
    if kind == "text":
        schema = "stfc_huawei_ngn_tim"
        source = tmp_path / directory / "records.csv"
        write_text(source, schema, values)
        extractor = CDRTextExtractor(spark)
    else:
        schema = "stfc_italtel_ngn_tim"
        source = tmp_path / directory / "records.parquet"
        mapping = PARQUET_DEFAULT_SCHEMAS[schema].column_mapping
        fields = T.StructType(
            [T.StructField(name, T.StringType()) for name, _ in mapping]
        )
        spark.createDataFrame(
            [tuple(values.get(target) for _, target in mapping)],
            fields,
        ).write.parquet(str(source))
        extractor = CDRParquetExtractor(spark)
    config = local_config(tmp_path, source, schema)
    config["pattern"] = "records.*"
    config["transformed_folder"] = str(tmp_path / ("output " + directory))
    original_timestamp_type = spark.conf.get("spark.sql.timestampType")
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    original_fs = hadoop_config.get("fs.defaultFS")
    try:
        # Protocolo inexistente: se file:/// for perdido, falha sem acesso à rede.
        hadoop_config.set("fs.defaultFS", "unsupported://synthetic")
        result = run_etl_item(config, spark=spark)
    finally:
        if original_fs is None:
            hadoop_config.unset("fs.defaultFS")
        else:
            hadoop_config.set("fs.defaultFS", original_fs)
    assert result.status == "success", result.error_message
    assert result.paths.source_file == f"file://{source.parent}/records.*"
    assert result.completed_stages == ("extraction", "transformation")
    assert spark.conf.get("spark.sql.timestampType") == original_timestamp_type
    assert spark.range(1).count() == 1

    intermediate = str(tmp_path / "direct_extracted")
    expected_path = str(tmp_path / "direct_transformed")
    extractor.extract(
        str(source),
        intermediate,
        schema,
        operator="synthetic",
        service="stfc",
        cdr_type="test",
    )
    getattr(CDRTransformer(spark), f"transform_{schema}")(intermediate, expected_path)
    actual = spark.read.parquet(result.paths.transformed_file)
    expected = spark.read.parquet(expected_path)
    assert actual.schema == expected.schema
    assert Counter(actual.collect()) == Counter(expected.collect())
    assert actual.count() == 1
    assert set(actual.columns) == {name for name, _ in TARGET_SCHEMA.values()}
    output = Path(config["transformed_folder"]) / "sample_transformed.parquet"
    assert list(output.glob("no_tipo_chamada=*"))
    record = actual.first()
    assert record.no_prestadora == "synthetic"
    assert record.no_esquema == schema
    assert record.nu_origem == "11987654321"
    assert record.qt_duracao_segundos == 60


def test_factory_and_real_catalog_methods(spark):
    components = etl._create_components(spark, {"text", "parquet"})
    assert all(extractor.spark is spark for extractor in components.extractors)
    assert components.transformer.spark is not spark
    assert components.transformer.spark.sparkContext is spark.sparkContext
    for schema in (*PARQUET_DEFAULT_SCHEMAS, *TEXT_DEFAULT_SCHEMAS):
        extractor, method = etl._resolve_operations(components, schema)
        assert schema in extractor.schemas
        assert callable(method)
    text_only = etl._create_components(spark, {"text"})
    assert len(text_only.extractors) == 1
    assert isinstance(text_only.extractors[0], CDRTextExtractor)


def test_legb_regression_awaits_independent_transformer_fix(spark, tmp_path):
    schema = "stfc_tropico_claro_legb"
    source = tmp_path / "input" / "legb.csv"
    write_text(
        source,
        schema,
        {
            "bilhetador": "synthetic",
            "_resultado_chamada": "003",
            "_data": "21102026",
            "_hora": "120000",
            "_data_fim": "21102026",
            "_hora_fim": "120100",
            "duracao": "60",
            "numero_origem": "11987654321",
            "numero_destino": "21987654321",
            "referencia": "synthetic-legb",
        },
    )
    config = local_config(tmp_path, source, schema)
    result = run_etl_item(config, spark=spark)
    if result.status == "execution_error":
        assert result.stage == "transformation"
        assert result.completed_stages == ("extraction",)
        assert result.error_type == "AnalysisException"
        assert "UNRESOLVED_COLUMN" in result.error_message
        assert "_tipo_chamada" in result.error_message
        pytest.xfail(
            "Correção independente pendente: transformador LEGB exige _tipo_chamada."
        )
    assert result.status == "success", result.error_message
    output = spark.read.parquet(result.paths.transformed_file)
    assert output.count() == 1
    assert set(output.columns) == {name for name, _ in TARGET_SCHEMA.values()}
    assert output.first().no_esquema == schema
