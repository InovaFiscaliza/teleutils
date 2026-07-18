"""Pacote robocalls: pipeline de detecção de padrões de chamadas abusivas.

Reúne as etapas de extração (``RoboCallsExtractor``), transformação
(``RoboCallsTransformer``) e análise (``RoboCallsAnalyzer``) de CDRs voltadas
especificamente à identificação de robocalls, com base em heurísticas de
duração de chamada, autenticação STIR/SHAKEN e padrões de encaminhamento ao
correio de voz.
"""

# O NullHandler já é registrado ao importar _logging.
# Aqui apenas garantimos que isso acontece ao carregar o pacote.
import teleutils._logging  # noqa: F401
from teleutils.robocalls.analyzers import RoboCallsAnalyzer
from teleutils.robocalls.extractors import RoboCallsExtractor
from teleutils.robocalls.transformers import RoboCallsTransformer

__all__ = ["RoboCallsExtractor", "RoboCallsTransformer", "RoboCallsAnalyzer"]
