"""Pacote de extratores de CDR do módulo core.

Reexporta as classes de extração disponíveis para os dois formatos de entrada
suportados: arquivos de texto/CSV (``CDRTextExtractor``) e parquet processado
pelo Teleparser (``CDRTeleparserExtractor``).
"""

from teleutils.core.extractors.teleparser_extractors import CDRTeleparserExtractor
from teleutils.core.extractors.text_extractors import CDRTextExtractor

__all__ = ["CDRTextExtractor", "CDRTeleparserExtractor"]
