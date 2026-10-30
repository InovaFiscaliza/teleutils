"""Pacote de extratores de CDR do módulo core.

Reexporta as classes de extração disponíveis para os dois formatos de entrada
suportados: arquivos de texto/CSV (``CDRTextExtractor``) e parquet processado
pelo Teleparser (``CDRParquetExtractor``). Este módulo não implementa o fluxo
de extração; ele oferece um ponto único de importação para as classes públicas
e restringe sua exportação explícita por meio de ``__all__``.

Exports:
        CDRTextExtractor: Extrator de layouts CDR em arquivos texto/CSV.
        CDRParquetExtractor: Extrator de layouts CDR em arquivos Parquet do
                Teleparser.

Example:
        >>> from teleutils.core.extractors import CDRTextExtractor
        >>> extrator = CDRTextExtractor(spark)

Notes:
        Os contratos de layout (``CDRParquetSchema``, ``CDRTextSchema`` e os
        catálogos padrão) pertencem a ``teleutils.core.extractors.schemas`` e
        não são reexportados aqui. ``_extractors_utils`` é de uso interno dos
        dois extratores.
"""

from teleutils.core.extractors.parquet_extractors import CDRParquetExtractor
from teleutils.core.extractors.text_extractors import CDRTextExtractor

__all__ = ["CDRParquetExtractor", "CDRTextExtractor"]
