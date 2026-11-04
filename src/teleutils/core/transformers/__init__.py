"""Pacote de transformadores de CDR do módulo core.

Este pacote reexporta ``CDRTransformer``, responsável por transformar CDRs
intermediários em Parquet ou DataFrame, produzidos pelos extratores de ``core.extractors``
(``CDRTextExtractor`` e ``CDRParquetExtractor``). A classe herda o pipeline
comum de ``CDRBaseTransformer`` e aplica pré-processamentos específicos de
fornecedor antes da persistência no contrato final.

O pacote não implementa transformações; ele fornece um ponto único de
importação e restringe a API pública por meio de ``__all__``.

Exports:
    CDRTransformer: Transformador de CDRs extraídos para Parquet final.

Notes:
    Os transformadores de layout são métodos ``transform_<layout>`` de
    ``CDRTransformer``. ``CDRTransformer.transform`` procura funções globais
    ``transform_<cdr_schema>`` no namespace de ``transformers.py``, não os
    métodos da instância; como esse módulo não define tais funções globais,
    consulte a docstring desse método antes de usá-lo como despachante.

Example:
    >>> from teleutils.core.transformers import CDRTransformer
    >>> transformer = CDRTransformer(spark)
"""

from teleutils.core.transformers.transformers import (
    CDRTransformer,
)

__all__ = ["CDRTransformer"]
