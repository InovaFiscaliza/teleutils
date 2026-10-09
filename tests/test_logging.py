from __future__ import annotations

import inspect
import logging
import traceback

import pytest

from teleutils._logging import format_source_file_for_log, log_operation


class Probe:
    def __init__(self, error=None):
        self.error = error
        self.called = False

    @log_operation
    def extract(self, source_file: str | list[str], *, marker: str = "default"):
        self.called = True
        if self.error is not None:
            raise self.error
        return source_file, marker


class ConsumerProbe(Probe):
    __module__ = "consumer.app"


@pytest.mark.parametrize(
    ("source_file", "expected"),
    [
        ("entrada.csv", "'entrada.csv'"),
        (["entrada.csv"], "'entrada.csv' (1 caminho)"),
        (["a.csv", "b.csv"], "'a.csv' (2 caminhos)"),
        ([], "[] (0 caminhos)"),
        ([None], "None (1 caminho)"),
        ("linha\nnova.csv", "'linha\\nnova.csv'"),
    ],
)
def test_format_source_file_for_log(source_file, expected):
    assert format_source_file_for_log(source_file) == expected


@pytest.mark.parametrize("source_file", ["entrada.csv", ["entrada.csv"], [], [None]])
def test_log_operation_preserves_arguments_and_return_value(caplog, source_file):
    probe = Probe()
    with caplog.at_level(logging.INFO, logger=__name__):
        result = probe.extract(source_file=source_file, marker="custom")

    assert probe.called
    assert result[0] is source_file
    assert result[1] == "custom"
    assert len(caplog.records) == 2
    for record in caplog.records:
        assert "Probe.extract" in record.getMessage()
        assert format_source_file_for_log(source_file) in record.getMessage()
    assert "concluída com sucesso" in caplog.records[-1].getMessage()
    assert "duração:" in caplog.records[-1].getMessage()


def test_log_operation_records_and_reraises_original_exception(caplog):
    error = ValueError("entrada vazia")
    probe = Probe(error)
    with pytest.raises(ValueError) as captured, caplog.at_level(logging.INFO):
        probe.extract([])

    assert probe.called
    assert captured.value is error
    assert len(caplog.records) == 2
    failure = caplog.records[-1]
    assert failure.levelno == logging.ERROR
    assert failure.exc_info[1] is error
    assert "[] (0 caminhos)" in failure.getMessage()
    assert "entrada vazia" not in failure.getMessage()
    assert "concluída com sucesso" not in caplog.text
    assert traceback.extract_tb(captured.value.__traceback__)[-1].name == "extract"


def test_log_operation_uses_implementation_module_for_inherited_method(caplog):
    with caplog.at_level(logging.INFO, logger=__name__):
        ConsumerProbe().extract("entrada.csv")

    assert len(caplog.records) == 2
    assert all(record.name == __name__ for record in caplog.records)


def test_log_operation_preserves_method_metadata_and_signature():
    assert Probe.extract.__name__ == "extract"
    assert inspect.signature(Probe.extract) == inspect.signature(
        Probe.extract.__wrapped__  # type: ignore
    )
