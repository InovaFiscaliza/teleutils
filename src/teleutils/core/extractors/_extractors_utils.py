"""Enriquecimento compartilhado de metadados dos extratores de CDR."""

from __future__ import annotations

import logging
from typing import Any, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_PATH_COLUMN = "__teleutils_source_path"


def read_text_with_source(
    spark: SparkSession, source_file: str | list[str]
) -> DataFrame:
    """Lê linhas via RDD, preservando o arquivo efetivo em uma coluna interna.

    Caminhos, diretórios e glob patterns são resolvidos pelo filesystem Hadoop.
    Arquivos e diretórios ocultos são ignorados, como nas leituras Spark.
    Cada arquivo é lido com ``textFile``; somente os caminhos são enumerados
    no driver, sem carregar o conteúdo inteiro dos arquivos em memória.
    """
    context = spark.sparkContext
    jvm = cast(Any, context._jvm)
    java_context = cast(Any, context._jsc)
    if jvm is None or java_context is None:
        raise RuntimeError("A sessão Spark não possui um contexto Hadoop ativo.")
    configuration = java_context.hadoopConfiguration()
    sources = source_file if isinstance(source_file, list) else [source_file]
    files = {}
    for source in sources:
        path = jvm.org.apache.hadoop.fs.Path(source)
        filesystem = path.getFileSystem(configuration)
        matches = filesystem.globStatus(path)
        if not matches:
            raise FileNotFoundError(f"Caminho de entrada não encontrado: {source!r}")
        pending = list(matches)
        while pending:
            status = pending.pop()
            current_path = status.getPath()
            if current_path.getName().startswith((".", "_")):
                continue
            if status.isDirectory():
                pending.extend(filesystem.listStatus(current_path))
            else:
                qualified = filesystem.makeQualified(current_path)
                files[qualified.toString()] = qualified.toUri().toASCIIString()

    rdds = [
        context.textFile(path).map(lambda line, origin=uri: (line, origin))
        for path, uri in files.items()
    ]
    lines = context.union(rdds) if rdds else context.emptyRDD()
    return spark.createDataFrame(lines, f"value string, {SOURCE_PATH_COLUMN} string")


def validate_metadata_arguments(
    operator: str | None, service: str | None, cdr_type: str | None
) -> None:
    """Valida tipos e espaços em branco, sem restringir os nomes permitidos.

    ``None`` e strings vazias indicam que o valor não foi informado.
    Strings válidas são preservadas sem normalização.

    Raises:
        TypeError: Se um valor informado não for uma string.
        ValueError: Se um valor contiver somente espaços em branco.
    """
    for name, value in (
        ("operator", operator),
        ("service", service),
        ("cdr_type", cdr_type),
    ):
        if value is None:
            continue
        if not isinstance(value, str):
            raise TypeError(f"{name} deve ser uma string ou None.")
        if value and not value.strip():
            raise ValueError(f"{name} não pode conter somente espaços em branco.")


def add_source_metadata(
    df: DataFrame,
    cdr_schema: str,
    *,
    operator: str | None = "",
    service: str | None = "",
    cdr_type: str | None = "",
    logger: logging.Logger | None = None,
) -> DataFrame:
    """Adiciona cinco metadados com argumentos prioritários sobre diretórios.

    A coluna interna ``SOURCE_PATH_COLUMN`` deve conter o arquivo efetivo de
    cada registro. A estrutura esperada é ``../operator/service/cdr_type/arquivo``.
    Somente campos não informados são inferidos e verificados. Não há validação
    de homogeneidade ou de nomes contra catálogos de valores permitidos.

    ``_tipo_cdr``, quando presente, prevalece inclusive com valores nulos. A
    coluna é preservada para filtros e deduplicação realizados pelo chamador.
    O caminho interno é removido do resultado. Nomes de arquivos e diretórios
    passam por decodificação de URI, preservando sinais de mais literais.

    A verificação de campos inferidos executa uma ação Spark antes da escrita.
    Quando nenhum campo depende do caminho, essa verificação é dispensada.

    Raises:
        TypeError: Se um argumento de metadados tiver tipo inválido.
        ValueError: Se faltar o caminho interno, um argumento contiver somente
            espaços ou não for possível inferir algum campo obrigatório.
    """
    validate_metadata_arguments(operator, service, cdr_type)
    if SOURCE_PATH_COLUMN not in df.columns:
        raise ValueError("O DataFrame não contém o caminho dos arquivos de origem.")

    path = F.parse_url(F.col(SOURCE_PATH_COLUMN), F.lit("PATH"))
    segments = F.split(path, "/")
    inferred_names = []
    expressions = {"esquema": F.lit(cdr_schema)}
    for name, value, position in (
        ("prestadora", operator, -4),
        ("servico", service, -3),
        ("tipo_cdr", cdr_type, -2),
    ):
        if name == "tipo_cdr" and "_tipo_cdr" in df.columns:
            expressions[name] = F.col("_tipo_cdr")
            operation_logger = (
                logger if logger is not None else logging.getLogger(__name__)
            )
            operation_logger.info(
                "Coluna _tipo_cdr encontrada nos dados; o argumento cdr_type "
                "ou a inferência pelo caminho foi ignorado."
            )
        elif value:
            expressions[name] = F.lit(value)
        else:
            segment = F.try_element_at(segments, F.lit(position))
            expressions[name] = F.url_decode(F.regexp_replace(segment, r"\+", "%2B"))
            inferred_names.append(name)

    filename = F.try_element_at(segments, F.lit(-1))
    expressions["arquivo_origem"] = F.url_decode(
        F.regexp_replace(filename, r"\+", "%2B")
    )
    df = df.withColumns(expressions)

    if inferred_names:
        invalid = F.lit(False)
        for name in inferred_names:
            invalid = invalid | F.col(name).isNull() | (F.trim(F.col(name)) == "")
        failures = (
            df.filter(invalid)
            .select(SOURCE_PATH_COLUMN, *inferred_names)
            .limit(1)
            .collect()
        )
        if failures:
            record = failures[0]
            missing = [
                name
                for name in inferred_names
                if not record[name] or not record[name].strip()
            ]
            raise ValueError(
                f"Não foi possível inferir {missing} do arquivo "
                f"{record[SOURCE_PATH_COLUMN]!r}. "
                "Use a estrutura ../operator/service/cdr_type/arquivo "
                "ou informe os argumentos correspondentes."
            )

    return df.drop(SOURCE_PATH_COLUMN)
