"""Contratos de schema usados pelos extratores de CDR."""

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
