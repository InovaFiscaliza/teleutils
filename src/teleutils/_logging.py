from __future__ import annotations

import logging
from functools import wraps
from typing import Callable

# Convenção para bibliotecas: NullHandler no pacote raiz.
# Evita o aviso "No handlers could be found" quando o consumidor
# não configura logging. A configuração real (handlers, formatters,
# níveis) é responsabilidade da aplicação, não da biblioteca.
logging.getLogger("teleutils").addHandler(logging.NullHandler())


def log_operation(method: Callable) -> Callable:
    """
    Decorador para registrar início, fim e falhas de métodos de transformação.

    Usa o logger do módulo da classe decorada (via self.__class__.__module__)
    para manter a hierarquia de loggers correta na biblioteca.
    """

    @wraps(method)
    def wrapper(self, source_file: str, *args, **kwargs):
        logger = logging.getLogger(self.__class__.__module__)
        logger.info("Iniciando operação [%s]: %s", method.__name__, source_file)
        try:
            result = method(self, source_file, *args, **kwargs)
            logger.info(
                "Operação [%s] concluída com sucesso.",
                method.__name__,
            )
            return result
        except Exception as e:
            logger.exception(
                "Falha na operação [%s]: %s %s",
                method.__name__,
                source_file,
                e,
            )
            raise

    return wrapper
