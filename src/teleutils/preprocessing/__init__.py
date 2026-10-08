"""API pública do pacote de pré-processamento.

Este inicializador reexporta as funções listadas em ``__all__`` para acesso
direto pelo namespace ``teleutils.preprocessing``:

        - ``normalize_number``: normaliza e valida um número telefônico brasileiro,
            conforme os padrões implementados em ``number_format``.
        - ``is_valid_cnpj``: valida um CNPJ conforme a implementação em ``utils``.

Example:
        >>> normalize_number("11999999999")
        ('11999999999', True)
        >>> is_valid_cnpj("11222333000181")
        True
"""

from teleutils.preprocessing.number_format import (
    normalize_number,
)
from teleutils.preprocessing.utils import is_valid_cnpj

__all__ = [
    "is_valid_cnpj",
    "normalize_number",
]
