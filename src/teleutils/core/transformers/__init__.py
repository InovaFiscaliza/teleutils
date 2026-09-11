"""Pacote de transformadores de CDR do módulo core.

Reexporta as classes de transformação disponíveis para CDRs extraídos via
Teleparser (``CDRTeleparserTransformer``) e via extração textual
(``CDRTextTransformer``), ambas derivadas do pipeline comum implementado em
``CDRBaseTransformer``.
"""

from teleutils.core.transformers.transformers import (
    CDRTeleparserTransformer,
)

__all__ = ["CDRTeleparserTransformer"]
