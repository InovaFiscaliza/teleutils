"""Interface pública dos contratos de schema usados pelos extratores de CDR.

Este pacote agrega os contratos declarativos empregados pelos extratores de
arquivos Parquet e texto/CSV. Ele reexporta as dataclasses de configuração e os
catálogos de schemas padrão para que consumidores possam importar os elementos
de schema a partir de um único local, sem depender da organização dos módulos
internos.

Exports:
    CDRParquetSchema: Contrato de mapeamento de colunas para CDRs Parquet.
    CDRTextSchema: Contrato de leitura, seleção e filtragem para CDRs texto/CSV.
    PARQUET_DEFAULT_SCHEMAS: Catálogo de contratos padrão para layouts Parquet.
    TEXT_DEFAULT_SCHEMAS: Catálogo de contratos padrão para layouts texto/CSV.

Example:
    >>> from teleutils.core.extractors.schemas import PARQUET_DEFAULT_SCHEMAS
    >>> PARQUET_DEFAULT_SCHEMAS["smp_ericsson_gsm"].name
    'SMP Ericsson GSM'
"""

from teleutils.core.extractors.schemas.parquet import (
    PARQUET_DEFAULT_SCHEMAS,
    CDRParquetSchema,
)
from teleutils.core.extractors.schemas.text import CDRTextSchema, TEXT_DEFAULT_SCHEMAS

__all__ = [
    "CDRParquetSchema",
    "CDRTextSchema",
    "PARQUET_DEFAULT_SCHEMAS",
    "TEXT_DEFAULT_SCHEMAS",
]
