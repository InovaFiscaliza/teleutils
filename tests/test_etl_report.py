import pytest

from teleutils.core.orchestrators import (
    ConfigPaths,
    ETLItemResult,
    ETLReport,
    print_etl_report,
)


def test_print_empty_report(capsys):
    assert print_etl_report(ETLReport(())) is None
    assert capsys.readouterr().out == (
        "=" * 60
        + "\nRELATÓRIO ETL\n"
        + "=" * 60
        + "\nTotal de itens:       0\n"
        + "Sucessos:             0\n"
        + "Erros de validação:   0\n"
        + "Erros de execução:    0\n"
        + "\nNenhum item processado.\n"
    )


def test_print_report_includes_success_and_failure_details(capsys):
    paths = ConfigPaths("schema", "s3a://bucket/input/*", "extracted", "transformed")
    report = ETLReport(
        (
            ETLItemResult(
                0,
                "schema",
                paths,
                "success",
                "completed",
                ("extraction", "transformation"),
            ),
            ETLItemResult(
                1,
                None,
                None,
                "validation_error",
                "configuration",
                error_type="ValueError",
                error_message="configuração inválida",
            ),
            ETLItemResult(
                2,
                "schema",
                paths,
                "execution_error",
                "transformation",
                ("extraction",),
                "RuntimeError",
                "falha na transformação",
            ),
        )
    )

    print_etl_report(report)
    output = capsys.readouterr().out
    assert "Total de itens:       3" in output
    assert "Sucessos:             1" in output
    assert "Erros de validação:   1" in output
    assert "Erros de execução:    1" in output
    assert output.index("Item:                 0") < output.index(
        "Item:                 1"
    )
    assert output.index("Item:                 1") < output.index(
        "Item:                 2"
    )
    assert "Schema:               -" in output
    assert "Status:               validation_error" in output
    assert "Etapa:                configuration" in output
    assert "Etapas concluídas:    -" in output
    assert "Etapas concluídas:    extraction, transformation" in output
    assert "Etapas concluídas:    extraction\n" in output
    assert "Origem:               s3a://bucket/input/*" in output
    assert "Parquet extraído:     extracted" in output
    assert "Parquet transformado: transformed" in output
    assert "Tipo do erro:         ValueError" in output
    assert "Mensagem do erro:     configuração inválida" in output
    assert "Tipo do erro:         RuntimeError" in output
    assert "Mensagem do erro:     falha na transformação" in output


@pytest.mark.parametrize(
    "item,counts",
    [
        (
            ETLItemResult(
                7,
                "schema",
                ConfigPaths("schema", "source", "extracted", "transformed"),
                "success",
                "completed",
                ("extraction", "transformation"),
            ),
            (1, 0, 0),
        ),
        (
            ETLItemResult(
                3,
                None,
                None,
                "validation_error",
                "configuration",
                error_type="ValueError",
                error_message="configuração inválida",
            ),
            (0, 1, 0),
        ),
        (
            ETLItemResult(
                5,
                "schema",
                None,
                "execution_error",
                "transformation",
                ("extraction",),
                "RuntimeError",
                "falha",
            ),
            (0, 0, 1),
        ),
    ],
)
def test_print_single_item_matches_one_item_report(capsys, item, counts):
    assert print_etl_report(item) is None
    single_output = capsys.readouterr().out
    print_etl_report(ETLReport((item,)))
    assert single_output == capsys.readouterr().out
    assert "Total de itens:       1" in single_output
    assert f"Sucessos:             {counts[0]}" in single_output
    assert f"Erros de validação:   {counts[1]}" in single_output
    assert f"Erros de execução:    {counts[2]}" in single_output
    assert f"Item:                 {item.index}" in single_output
    assert f"Status:               {item.status}" in single_output
    if item.error_message is not None:
        assert f"Mensagem do erro:     {item.error_message}" in single_output
    if item.paths is not None:
        assert f"Origem:               {item.paths.source_file}" in single_output


@pytest.mark.parametrize("value", [None, [], {}])
def test_print_report_rejects_invalid_input_before_printing(capsys, caplog, value):
    with pytest.raises(TypeError, match="ETLReport ou ETLItemResult"):
        print_etl_report(value)
    assert capsys.readouterr().out == ""
    assert "report deve ser ETLReport ou ETLItemResult" in caplog.text
