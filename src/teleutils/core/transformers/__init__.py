"""Pacote de transformadores de CDR do módulo core.

Reexporta as classes de transformação disponíveis para CDRs extraídos via
Teleparser (``CDRTransformer``) e via extração textual
(``CDRTextTransformer``), ambas derivadas do pipeline comum implementado em
``CDRBaseTransformer``.
"""

from teleutils.core.transformers.transformers import (
    CDRTransformer,
)

__all__ = ["CDRTransformer"]
