"""Pacote de transformadores de CDR do módulo core.

Este pacote reexporta ``CDRTransformer``, responsável por transformar CDRs
intermediários extraídos via Teleparser. A classe herda o pipeline comum de
``CDRBaseTransformer`` e aplica pré-processamentos específicos de fornecedor
antes da persistência no contrato final.

O pacote não implementa transformações; ele fornece um ponto único de
importação e restringe a API pública por meio de ``__all__``.

Exports:
    CDRTransformer: Transformador de CDRs Parquet extraídos pelo Teleparser.

Example:
    >>> from teleutils.core.transformers import CDRTransformer
    >>> transformer = CDRTransformer(spark)
"""

from teleutils.core.transformers.transformers import (
    CDRTransformer,
)

__all__ = ["CDRTransformer"]
