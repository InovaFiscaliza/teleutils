"""Utilitários internos para resolução de contratos de schema dos extratores."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeVar

SchemaT = TypeVar("SchemaT")


def resolve_cdr_schema(schemas: Mapping[str, SchemaT], cdr_schema: str) -> SchemaT:
    """Obtém um contrato pela chave exata, sem modificar o catálogo ou o contrato.

    Args:
        schemas: Catálogo de contratos indexados por chave de layout.
        cdr_schema: Chave textual, sem normalização de espaços ou maiúsculas.

    Returns:
        O contrato associado à chave, preservando seu tipo e identidade.

    Raises:
        TypeError: Se ``cdr_schema`` não for uma string.
        ValueError: Se a chave não estiver no catálogo informado.
    """
    if not isinstance(cdr_schema, str):
        raise TypeError(
            f"cdr_schema deve ser uma string. Recebido: {type(cdr_schema).__name__}"
        )
    try:
        return schemas[cdr_schema]
    except KeyError:
        raise ValueError(
            f"Schema '{cdr_schema}' não encontrado. "
            f"Schemas disponíveis: {list(schemas)}"
        ) from None
