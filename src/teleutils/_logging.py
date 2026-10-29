from __future__ import annotations

import logging
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Convenção para bibliotecas: NullHandler no pacote raiz.
# Evita o aviso "No handlers could be found" quando o consumidor
# não configura logging. A configuração real (handlers, formatters,
# níveis) é responsabilidade da aplicação, não da biblioteca.
logging.getLogger("teleutils").addHandler(logging.NullHandler())


Method = TypeVar("Method", bound=Callable[..., Any])


def format_source_file_for_log(source_file: str | list[str] | DataFrame) -> str:
    """Descreve a entrada para log, sem validar ou modificar os caminhos.

    Listas são resumidas pelo primeiro elemento e pela quantidade de caminhos.
    Listas vazias são representadas sem acesso ao primeiro elemento. A
    representação por ``repr`` torna caracteres de controle visíveis.
    """
    if isinstance(source_file, list):
        count = len(source_file)
        first_path = repr(source_file[0]) if source_file else "[]"
        label = "caminho" if count == 1 else "caminhos"
        return f"{first_path} ({count} {label})"
    if isinstance(source_file, str):
        return repr(source_file)
    # Não importar Spark nem inspecionar o plano ao descrever uma entrada.
    return f"<{type(source_file).__name__}>"


def log_operation(method: Method) -> Method:
    """Registra início, sucesso, duração e falhas de extração, transformação e carga.

    Usa o logger do módulo que implementa o método e seu nome qualificado.
    Não valida a entrada nem configura níveis ou handlers da aplicação.
    Exceções da operação são registradas com traceback e relançadas intactas.
    A duração mede a execução do método, sem materializar ações Spark adicionais.
    """

    @wraps(method)
    def wrapper(
        self: Any, source_file: str | list[str] | DataFrame, *args: Any, **kwargs: Any
    ) -> Any:
        source_file_log = format_source_file_for_log(source_file)
        logger = logging.getLogger(method.__module__)
        operation = method.__qualname__
        logger.info("Iniciando operação [%s]: %s", operation, source_file_log)
        started_at = perf_counter()
        try:
            result = method(self, source_file, *args, **kwargs)
        except Exception:
            logger.exception(
                "Falha na operação [%s]: %s (duração: %.3f s)",
                operation,
                source_file_log,
                perf_counter() - started_at,
            )
            raise
        success_message = (
            "Plano [%s] preparado, sem persistência: %s (preparação/validação: %.3f s)"
            if method.__name__ == "extract_dataframe"
            else "Operação [%s] concluída com sucesso: %s (duração: %.3f s)"
        )
        logger.info(
            success_message,
            operation,
            source_file_log,
            perf_counter() - started_at,
        )
        return result

    return cast(Method, wrapper)
