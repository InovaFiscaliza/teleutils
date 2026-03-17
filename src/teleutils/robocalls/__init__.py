# O NullHandler já é registrado ao importar _logging.
# Aqui apenas garantimos que isso acontece ao carregar o pacote.
import teleutils._logging  # noqa: F401
from teleutils.robocalls.analyzers import RoboCallsAnalyzer
from teleutils.robocalls.extractors import RoboCallsExtractor
from teleutils.robocalls.transformers import RoboCallsTransformer

__all__ = ["RoboCallsExtractor", "RoboCallsTransformer", "RoboCallsAnalyzer"]
