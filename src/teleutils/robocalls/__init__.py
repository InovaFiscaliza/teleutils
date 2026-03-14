# O NullHandler já é registrado ao importar _logging.
# Aqui apenas garantimos que isso acontece ao carregar o pacote.
import teleutils._logging  # noqa: F401
from teleutils.robocalls._extractors import RoboCallsExtractor
from teleutils.robocalls._transformers import RoboCallsTransformer

__all__ = ["RoboCallsExtractor", "RoboCallsTransformer"]
