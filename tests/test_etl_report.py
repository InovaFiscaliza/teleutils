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
