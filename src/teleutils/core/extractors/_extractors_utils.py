"""Enriquecimento compartilhado de metadados dos extratores de CDR.

Funções de apoio usadas por ``CDRTextExtractor`` e ``CDRParquetExtractor``:

* ``suppress_spark_loggers``: decorator que reduz temporariamente o nível de
  loggers Log4j2 do Spark durante a execução de um método de extração
  (aplicado em ``CDRParquetExtractor.extract``).
* ``read_text_with_source``: leitura de linhas via RDD que preserva o arquivo
  de origem de cada linha (usada quando ``read_rdd=True``).
* ``validate_metadata_arguments``: validação de ``operator``, ``service`` e
  ``cdr_type``.
* ``add_source_metadata``: adição das colunas ``esquema``, ``prestadora``,
  ``servico``, ``tipo_cdr`` e ``arquivo_origem`` ao DataFrame extraído.

Fluxo de metadados: o extrator garante a coluna interna
``SOURCE_PATH_COLUMN`` (caminho do arquivo de cada registro), mantém-na
na seleção de colunas e, ao final, ``add_source_metadata`` deriva os metadados
a partir dela e a remove do resultado.

Dependências:
    - ``pyspark``: DataFrames, funções SQL e acesso à JVM (Log4j2 e Hadoop).
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DecoratorMethod = TypeVar("DecoratorMethod", bound=Callable[..., Any])


# Coluna interna que carrega o caminho do arquivo de origem de cada registro.
# É criada pela leitura (``read_text_with_source`` ou ``input_file_name``),
# consumida por ``add_source_metadata`` e removida do resultado final.
SOURCE_PATH_COLUMN = "__teleutils_source_path"


def suppress_spark_loggers(
    *logger_names: str,
    level: str = "ERROR",
) -> Callable[[DecoratorMethod], DecoratorMethod]:
    """Altera níveis Spark e registra aplicação e restauração no logger Python.

    Registra em INFO os nomes e níveis no logger do módulo do método decorado.
    Não captura as mensagens descartadas pelo Spark. Restaura os níveis
    explícitos e a herança anteriores no finally, inclusive quando o método
    falha. A alteração afeta o contexto Log4j2 compartilhado da JVM do driver;
    não há isolamento entre chamadas concorrentes.

    Args:
        *logger_names: Nomes dos loggers Log4j2 a alterar. Ao menos um é
            obrigatório.
        level: Nome do nível temporário (sem diferenciar maiúsculas de
            minúsculas), resolvido em ``log4j.Level``. O padrão é ``"ERROR"``.

    Returns:
        Decorator para métodos de instância. O wrapper acessa a JVM por
        ``self.spark.sparkContext``, portanto o objeto decorado deve ter o
        atributo ``spark``. O resultado do método é repassado inalterado.

    Raises:
        ValueError: Se nenhum nome de logger for informado (levantada ao
            aplicar a fábrica, antes da decoração).
    """
    if not logger_names:
        raise ValueError("Informe ao menos um logger.")

    def decorate(method: DecoratorMethod) -> DecoratorMethod:
        @wraps(method)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            operation_logger = logging.getLogger(method.__module__)
            jvm = self.spark.sparkContext._jvm
            log4j = jvm.org.apache.logging.log4j
            configurator = log4j.core.config.Configurator
            target_level = getattr(log4j.Level, level.upper())
            context = log4j.LogManager.getContext(False)
            configuration = context.getConfiguration()

            previous_configs = {}
            for name in logger_names:
                logger_config = configuration.getLoggers().get(name)
                previous_configs[name] = (
                    logger_config,
                    logger_config.getExplicitLevel()
                    if logger_config is not None
                    else None,
                    log4j.LogManager.getLogger(name).getLevel(),
                )

            try:
                for name in logger_names:
                    configurator.setLevel(name, target_level)
                    operation_logger.info(
                        "Logger Spark '%s': nível temporário %s aplicado "
                        "(nível anterior: %s).",
                        name,
                        target_level,
                        previous_configs[name][2],
                    )

                return method(self, *args, **kwargs)
            finally:
                for name, (
                    logger_config,
                    explicit_level,
                    _,
                ) in previous_configs.items():
                    # Logger sem configuração própria antes da execução tem a
                    # configuração temporária removida; os demais voltam ao
                    # nível explícito anterior.
                    if logger_config is None:
                        configuration.removeLogger(name)
                    else:
                        logger_config.setLevel(explicit_level)
                context.updateLoggers()
                for name in previous_configs:
                    operation_logger.info(
                        "Logger Spark '%s': configuração anterior restaurada "
                        "(nível efetivo: %s).",
                        name,
                        log4j.LogManager.getLogger(name).getLevel(),
                    )

        return cast(DecoratorMethod, wrapper)

    return decorate


def read_text_with_source(
    spark: SparkSession, source_file: str | list[str]
) -> DataFrame:
    """Lê linhas via RDD, preservando o arquivo efetivo em uma coluna interna.

    Caminhos, diretórios e glob patterns são resolvidos pelo filesystem Hadoop.
    Arquivos e diretórios ocultos são ignorados, como nas leituras Spark.
    Cada arquivo é lido com ``textFile``; somente os caminhos são enumerados
    no driver, sem carregar o conteúdo inteiro dos arquivos em memória.
    Um mesmo arquivo alcançado por mais de um caminho é lido uma única vez
    (deduplicação pelo caminho qualificado) e a ordem de união dos arquivos
    segue a enumeração do filesystem, sem ordenação explícita.
    Args:
        spark: Sessão Spark cujo contexto Hadoop resolve os caminhos.
        source_file: Caminho, diretório ou glob, ou lista deles.

    Returns:
        DataFrame com as colunas ``value`` (linha de texto) e
        ``SOURCE_PATH_COLUMN`` (URI do arquivo de origem em ASCII). O DataFrame
        é criado vazio, com esse schema, se nenhum arquivo for encontrado em
        diretórios existentes.

    Raises:
        RuntimeError: Se a sessão não possuir JVM ou contexto Java ativo.
        FileNotFoundError: Se algum caminho informado não corresponder a
            nenhum item no filesystem.
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

    Args:
        operator: Prestadora informada pelo chamador, ou ``None``/vazio.
        service: Serviço informado pelo chamador, ou ``None``/vazio.
        cdr_type: Tipo de CDR informado pelo chamador, ou ``None``/vazio.

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

    Uma coluna ``arquivo_origem`` já existente em ``df`` (mapeada pelo contrato
    do layout) é mantida; o nome do arquivo só é inferido do caminho quando ela
    não existe.
    Args:
        df: DataFrame extraído, com a coluna ``SOURCE_PATH_COLUMN``.
        cdr_schema: Chave do schema, gravada na coluna ``esquema``.
        operator: Valor de ``prestadora``; se vazio, é inferido do quarto
            segmento a partir do fim do caminho.
        service: Valor de ``servico``; se vazio, é inferido do terceiro
            segmento a partir do fim.
        cdr_type: Valor de ``tipo_cdr``; se vazio, é inferido do penúltimo
            segmento do caminho.
        logger: Logger usado ao registrar que ``_tipo_cdr`` prevaleceu; se
            ``None``, usa o logger do módulo.

    Returns:
        DataFrame com as colunas ``esquema``, ``prestadora``, ``servico`` e
        ``tipo_cdr`` e, se ainda não existir, ``arquivo_origem`` (nome do
        arquivo extraído do caminho). ``SOURCE_PATH_COLUMN`` é removida.

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

    # Se arquivo origem existe no DataFrame, não é necessário inferi-lo do caminho.
    if "arquivo_origem" not in df.columns:
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
