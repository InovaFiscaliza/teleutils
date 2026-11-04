import logging
import subprocess
import sys
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    TEXT_DEFAULT_SCHEMAS,
)
from teleutils.core.orchestrators import ETLReport, etl, run_etl, run_etl_item


def make_config(prefix="item", schema="stfc_huawei_ngn"):
    return {
        "source_folder": "bucket/source/operator/service/type",
        "pattern": "*.gz",
        "schema": schema,
        "extracted_folder": "bucket/extracted",
        "transformed_folder": "bucket/transformed",
        "output_file_prefix": prefix,
    }


@pytest.fixture
def harness(monkeypatch):
    events = []
    failures = {}

    class Extractor:
        def __init__(self, kind, schemas):
            self.kind = kind
            self.schemas = schemas

        def extract(self, source, target, schema, **metadata):
            events.append(("extract", self.kind, schema, source, target, metadata))
            if ("extract", schema) in failures:
                raise failures["extract", schema]
            return target

        def extract_dataframe(self, source, schema, **metadata):
            events.append(("extract_dataframe", self.kind, schema, source, metadata))
            if ("extract_dataframe", schema) in failures:
                raise failures["extract_dataframe", schema]
            return frame

    frame = object()
    transformer = SimpleNamespace()

    def method(schema):
        def transform(source, target):
            events.append(("transform", schema, source, target))
            if ("transform", schema) in failures:
                raise failures["transform", schema]
            return target

        return transform

    for schema in (*PARQUET_DEFAULT_SCHEMAS, *TEXT_DEFAULT_SCHEMAS):
        setattr(transformer, f"transform_{schema}", method(schema))
    parquet = Extractor("parquet", dict(PARQUET_DEFAULT_SCHEMAS))
    text = Extractor("text", dict(TEXT_DEFAULT_SCHEMAS))
    factory = Mock(return_value=etl._Components((parquet, text), transformer))
    monkeypatch.setattr(etl, "_create_components", factory)
    return SimpleNamespace(
        events=events,
        failures=failures,
        transformer=transformer,
        factory=factory,
        parquet=parquet,
        text=text,
        spark=Mock(),
        frame=frame,
    )


@pytest.mark.parametrize("schema", [*PARQUET_DEFAULT_SCHEMAS, *TEXT_DEFAULT_SCHEMAS])
def test_all_catalog_schemas_select_and_resolve(harness, schema):
    result = run_etl_item(make_config(schema=schema), spark=harness.spark)
    kind = "parquet" if schema in PARQUET_DEFAULT_SCHEMAS else "text"
    assert result.status == "success"
    assert result.index == 0
    assert result.completed_stages == ("extraction", "transformation")
    assert result.error_type is result.error_message is None
    assert harness.events == [
        (
            "extract",
            kind,
            schema,
            "s3a://bucket/source/operator/service/type/*.gz",
            "s3a://bucket/extracted/item_extracted.parquet",
            {"operator": None, "service": None, "cdr_type": None},
        ),
        (
            "transform",
            schema,
            "s3a://bucket/extracted/item_extracted.parquet",
            "s3a://bucket/transformed/item_transformed.parquet",
        ),
    ]
    harness.factory.assert_called_once_with(
        harness.spark, {kind}, materialize_intermediate=True
    )
    assert harness.spark.mock_calls == []


def test_metadata_is_forwarded_without_changes(harness):
    config = make_config()
    config.update(operator="custom operator", service="", cdr_type=None)
    run_etl_item(config, spark=harness.spark)
    assert harness.events[0][-1] == {
        "operator": "custom operator",
        "service": "",
        "cdr_type": None,
    }


def test_direct_pipeline_passes_frame_and_ignores_unused_destination(harness):
    config = make_config()
    config["extracted_folder"] = config["source_folder"]
    result = run_etl_item(config, spark=harness.spark, materialize_intermediate=False)
    assert result.status == "success"
    assert result.paths.extracted_file is None
    assert result.completed_stages == ("extraction", "transformation")
    assert harness.events[0][0] == "extract_dataframe"
    assert harness.events[1][0] == "transform"
    assert harness.events[1][2] is harness.frame
    harness.factory.assert_called_once_with(
        harness.spark, {"text"}, materialize_intermediate=False
    )


@pytest.mark.parametrize("stage", ["extract_dataframe", "transform"])
def test_direct_failure_never_certifies_extraction(harness, stage):
    config = make_config()
    del config["extracted_folder"]
    harness.failures[stage, config["schema"]] = ValueError("dados inválidos")
    report = run_etl(
        [config, make_config("next", "stfc_fcdr_vivo")],
        spark=harness.spark,
        materialize_intermediate=False,
    )
    failure, success = report.items
    assert failure.status == "execution_error"
    assert failure.stage == (
        "extraction" if stage == "extract_dataframe" else "pipeline"
    )
    assert failure.completed_stages == ()
    assert failure.error_message == "dados inválidos"
    assert success.status == "success"


def test_direct_destinations_cannot_overwrite_any_source(harness):
    first = make_config("first")
    second = make_config("second")
    second["source_folder"] = "bucket/transformed/first_transformed.parquet/input"
    report = run_etl(
        [first, second], spark=harness.spark, materialize_intermediate=False
    )
    assert all(item.stage == "paths" for item in report.items)
    harness.factory.assert_not_called()


@pytest.mark.parametrize("invalid", [None, "false", 0, 1, [], {}])
def test_invalid_materialization_mode_fails_before_initialization(harness, invalid):
    with pytest.raises(TypeError, match="materialize_intermediate"):
        run_etl_item(
            make_config(), spark=harness.spark, materialize_intermediate=invalid
        )
    harness.factory.assert_not_called()


@pytest.mark.parametrize("item", [None, "config", {}, {"schema": []}])
def test_bad_configuration_never_creates_components(harness, item, caplog):
    result = run_etl_item(item, spark=harness.spark)
    assert (result.status, result.stage) == ("validation_error", "configuration")
    assert result.paths is None
    assert result.completed_stages == ()
    assert "Configuração inválida" in caplog.text
    harness.factory.assert_not_called()


@pytest.mark.parametrize(
    "schema", ["unknown", " STFC_HUAWEI_NGN ", "stfc_tropico_claro"]
)
def test_unknown_schema_fails_before_components(harness, schema):
    result = run_etl_item(make_config(schema=schema), spark=harness.spark)
    assert (result.status, result.stage) == ("validation_error", "resolution")
    harness.factory.assert_not_called()


def test_ambiguous_schema_fails_before_components(harness, monkeypatch):
    monkeypatch.setitem(PARQUET_DEFAULT_SCHEMAS, "stfc_huawei_ngn", object())
    result = run_etl_item(make_config(), spark=harness.spark)
    assert result.status == "validation_error"
    assert "ambíguo" in result.error_message
    harness.factory.assert_not_called()


@pytest.mark.parametrize("value", [None, "not callable"])
def test_missing_transformer_is_checked_before_extraction(harness, value):
    harness.transformer.transform_stfc_huawei_ngn = value
    result = run_etl_item(make_config(), spark=harness.spark)
    assert (result.status, result.stage) == ("validation_error", "resolution")
    assert harness.events == []


def test_unavailable_extractor_is_rejected(harness):
    harness.text.schemas.clear()
    result = run_etl_item(make_config(), spark=harness.spark)
    assert (result.status, result.stage) == ("validation_error", "resolution")
    assert harness.events == []


@pytest.mark.parametrize("stage", ["extract", "transform"])
def test_failure_continuity_counts_and_logs(harness, stage, caplog):
    schema = "stfc_fcdr_vivo"
    harness.failures[stage, schema] = ValueError("dados inválidos")
    with caplog.at_level(logging.INFO):
        report = run_etl(
            [
                make_config("first"),
                make_config("failed", schema),
                make_config("last"),
                {"schema": "invalid"},
            ],
            spark=harness.spark,
        )
    assert [item.index for item in report.items] == [0, 1, 2, 3]
    assert [item.status for item in report.items] == [
        "success",
        "execution_error",
        "success",
        "validation_error",
    ]
    failure = report.items[1]
    assert (
        failure.stage == {"extract": "extraction", "transform": "transformation"}[stage]
    )
    assert failure.completed_stages == (() if stage == "extract" else ("extraction",))
    assert failure.error_type == "ValueError"
    assert failure.error_message == "dados inválidos"
    assert (
        report.total
        == report.successes + report.validation_errors + report.execution_errors
        == 4
    )
    assert (report.successes, report.validation_errors, report.execution_errors) == (
        2,
        1,
        1,
    )
    assert "sucessos=2" in caplog.text
    assert any(record.exc_info for record in caplog.records)
    harness.factory.assert_called_once()
    if stage == "extract":
        assert not any(event[:2] == ("transform", schema) for event in harness.events)
    with pytest.raises(FrozenInstanceError):
        report.items = ()
    with pytest.raises(FrozenInstanceError):
        failure.status = "success"


def test_initialization_failure_is_reported_for_each_valid_item(harness, caplog):
    harness.factory.side_effect = RuntimeError("sessão indisponível")
    report = run_etl([make_config("a"), {}, make_config("b")], spark=harness.spark)
    assert [item.stage for item in report.items] == [
        "initialization",
        "configuration",
        "initialization",
    ]
    assert report.execution_errors == 2
    assert report.validation_errors == 1
    assert "Falha na inicialização" in caplog.text
    assert harness.events == []


@pytest.mark.parametrize("exception", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("stage", ["extract", "transform"])
def test_interruptions_propagate(harness, exception, stage):
    harness.failures[stage, "stfc_huawei_ngn"] = exception()
    with pytest.raises(exception):
        run_etl_item(make_config(), spark=harness.spark)


def test_empty_batch_does_not_initialize(harness):
    assert run_etl([], spark=harness.spark) == ETLReport(())
    harness.factory.assert_not_called()


def test_source_folder_can_be_prefixed_with_base_and_week(harness):
    config = make_config()
    config["source_folder"] = "operator/service/type"
    original_config = config.copy()

    report = run_etl(
        [config],
        spark=harness.spark,
        base_folder="s3a://bucket/cdr/",
        week_dir="/2026-W01/",
    )

    assert report.items[0].paths.source_file == (
        "s3a://bucket/cdr/2026-W01/operator/service/type/*.gz"
    )
    assert config == original_config


@pytest.mark.parametrize(
    "storage,base,expected",
    [
        (
            "s3a",
            "s3a://bucket/cdr/",
            "s3a://bucket/cdr/2026-W01/operator/service/type/*.gz",
        ),
        (
            "local",
            "/dados/cdr/",
            "file:///dados/cdr/2026-W01/operator/service/type/*.gz",
        ),
    ],
)
def test_single_item_can_prefix_source_folder(harness, storage, base, expected):
    config = make_config()
    config["source_folder"] = "operator/service/type"
    if storage == "local":
        config.update(
            storage_type="local",
            extracted_folder="/dados/extracted",
            transformed_folder="/dados/transformed",
        )
    original_config = config.copy()

    result = run_etl_item(
        config, spark=harness.spark, base_folder=base, week_dir="/2026-W01/"
    )

    assert result.status == "success"
    assert result.index == 0
    assert result.paths.source_file == expected
    assert harness.events[0][3] == expected
    assert config == original_config


@pytest.mark.parametrize(
    "kwargs,error,message",
    [
        ({"base_folder": "bucket/cdr"}, ValueError, "informados em conjunto"),
        ({"week_dir": "2026-W01"}, ValueError, "informados em conjunto"),
        ({"base_folder": "", "week_dir": "week"}, ValueError, "base_folder"),
        ({"base_folder": "bucket", "week_dir": ""}, ValueError, "week_dir"),
        ({"base_folder": 1, "week_dir": "week"}, TypeError, "base_folder"),
        ({"base_folder": "bucket", "week_dir": 1}, TypeError, "week_dir"),
    ],
)
def test_single_item_reuses_prefix_argument_validation(harness, kwargs, error, message):
    with pytest.raises(error, match=message):
        run_etl_item(make_config(), spark=harness.spark, **kwargs)
    harness.factory.assert_not_called()
    assert harness.events == []


@pytest.mark.parametrize(
    "kwargs", [{"base_folder": "bucket/cdr"}, {"week_dir": "2026-W01"}]
)
def test_source_folder_prefix_arguments_must_be_provided_together(
    harness, kwargs, caplog
):
    with pytest.raises(ValueError, match="informados em conjunto"):
        run_etl([make_config()], spark=harness.spark, **kwargs)
    assert "base_folder e week_dir" in caplog.text
    harness.factory.assert_not_called()


@pytest.mark.parametrize("configs", [None, {}, "item", b"item", iter([])])
def test_invalid_batch_container_is_logged_and_raised(harness, configs, caplog):
    with pytest.raises(TypeError, match="sequência"):
        run_etl(configs, spark=harness.spark)
    assert "configs deve ser" in caplog.text
    harness.factory.assert_not_called()


def test_duplicate_prefixes_with_distinct_destinations_can_run(harness):
    first, second = make_config("repeated"), make_config("repeated")
    second["extracted_folder"] = "other-bucket/extracted"
    second["transformed_folder"] = "other-bucket/transformed"
    report = run_etl([first, make_config("independent"), second], spark=harness.spark)
    assert report.successes == 3
    assert len(harness.events) == 6


def test_duplicate_prefixes_with_same_destinations_still_conflict(harness):
    configs = [make_config("same"), make_config("same")]
    report = run_etl(configs, spark=harness.spark)
    assert [item.stage for item in report.items] == ["paths", "paths"]
    harness.factory.assert_not_called()


@pytest.mark.parametrize("case", ["nested", "source", "encoding", "slashes"])
def test_conflicting_items_rejected_and_independent_item_runs(harness, case):
    first, second = make_config("a"), make_config("b")
    if case == "nested":
        second["transformed_folder"] = "bucket/extracted/a_extracted.parquet/sub"
    elif case == "source":
        second["source_folder"] = "bucket/transformed/a_transformed.parquet/sub"
    elif case == "encoding":
        second["extracted_folder"] = "bucket/%65xtracted/a_extracted.parquet/sub"
    else:
        second["extracted_folder"] = "bucket//extracted/a_extracted.parquet/sub"
    report = run_etl([first, second, make_config("independent")], spark=harness.spark)
    assert [item.status for item in report.items] == [
        "validation_error",
        "validation_error",
        "success",
    ]
    assert [item.stage for item in report.items[:2]] == ["paths", "paths"]
    assert len(harness.events) == 2
    assert "independent" in harness.events[0][4]


def test_own_output_cannot_overlap_input(harness):
    config = make_config()
    config["extracted_folder"] = config["source_folder"]
    assert run_etl_item(config, spark=harness.spark).stage == "paths"
    harness.factory.assert_not_called()


def test_similar_sibling_paths_are_not_conflicts(harness):
    first, second = make_config("a"), make_config("ab")
    assert run_etl([first, second], spark=harness.spark).successes == 2


def test_all_operations_are_resolved_before_any_extraction(harness, monkeypatch):
    resolve = etl._resolve_operations
    events = []

    def track(components, schema):
        assert harness.events == []
        events.append(schema)
        return resolve(components, schema)

    monkeypatch.setattr(etl, "_resolve_operations", track)
    run_etl([make_config("a"), make_config("b")], spark=harness.spark)
    assert len(events) == 2


def test_public_import_has_no_spark_or_logging_side_effects():
    code = """
import logging
import sys
root = logging.getLogger()
before = list(root.handlers)
import teleutils.core.orchestrators as api
assert set(api.__all__) == {
    'ConfigPaths', 'get_config', 'load_config', 'ETLItemResult', 'ETLReport',
    'print_etl_report', 'run_etl', 'run_etl_item'
}
assert 'pyspark' not in sys.modules
assert 'minio' not in sys.modules
assert 'teleutils.core.transformers' not in sys.modules
assert 'teleutils.core.loaders' not in sys.modules
assert root.handlers == before
"""
    subprocess.run(
        [sys.executable, "-c", code], check=True, capture_output=True, text=True
    )


def as_local(config):
    return {
        **config,
        "storage_type": "local",
        **{
            key: "/" + config[key]
            for key in (
                "source_folder",
                "extracted_folder",
                "transformed_folder",
            )
        },
    }


@pytest.mark.parametrize("case", ["nested", "source", "slashes"])
def test_local_conflicts_before_writes(harness, case):
    first, second = as_local(make_config("a")), as_local(make_config("b"))
    if case == "nested":
        second["transformed_folder"] = "/bucket/extracted/a_extracted.parquet/sub"
    elif case == "source":
        second["source_folder"] = "/bucket/transformed/a_transformed.parquet/sub"
    else:
        second["extracted_folder"] = "/bucket//extracted/a_extracted.parquet///sub"
    report = run_etl([first, second], spark=harness.spark)
    assert [item.stage for item in report.items] == ["paths", "paths"]
    assert report.validation_errors == 2
    harness.factory.assert_not_called()
    assert harness.events == []


def test_mixed_storage_batch_reuses_components_and_keeps_results(harness):
    configs = [
        make_config(),
        as_local(make_config("local")),
        as_local(make_config("invalid")),
    ]
    configs[2]["transformed_folder"] = "s3a://bucket/output"
    report = run_etl(configs, spark=harness.spark)
    assert [item.status for item in report.items] == [
        "success",
        "success",
        "validation_error",
    ]
    assert report.items[0].paths.source_file.startswith("s3a://")
    assert report.items[1].paths.source_file.startswith("file:///")
    assert harness.events[2][3].startswith("file:///")
    assert harness.events[3][3].startswith("file:///")
    harness.factory.assert_called_once()
    assert (
        report.total
        == report.successes + report.validation_errors + report.execution_errors
    )


@pytest.mark.parametrize("storage", ["s3a", "local"])
def test_execution_failures_keep_processing_for_each_storage(harness, storage):
    configs = [make_config("fail", "stfc_fcdr_vivo"), make_config("success")]
    if storage == "local":
        configs = [as_local(config) for config in configs]
    harness.failures["transform", "stfc_fcdr_vivo"] = RuntimeError("synthetic failure")
    report = run_etl(configs, spark=harness.spark)
    assert [item.status for item in report.items] == ["execution_error", "success"]
    assert report.items[0].completed_stages == ("extraction",)


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("s3a://bucket/a", "file:///bucket/a", False),
        ("s3a://one/a", "s3a://two/a", False),
        ("file:///a", "file:///ab", False),
        ("file:///a", "file:///a/b", True),
        ("file:///", "file:///a", True),
        ("file:///a%20b", "file:///a b", False),
        ("file:///a#b", "file:///a#b/c", True),
        ("s3a://bucket/a%20b", "s3a://bucket/a b", True),
    ],
)
def test_overlap_respects_storage_and_local_literal_names(left, right, expected):
    assert etl._overlap(left, right) is expected
