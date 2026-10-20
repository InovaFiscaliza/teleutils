"""API pública do pacote de pré-processamento.

Este inicializador reexporta as funções listadas em ``__all__`` para acesso
direto pelo namespace ``teleutils.preprocessing``:

        - ``extract_number``: extrai e valida um número telefônico brasileiro,
            conforme os padrões implementados em ``number_format``. Retorna
            um ``ExtractionResult`` com o número e um indicador de sucesso;
            em caso de falha, o número é o valor sentinela
            ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``.
        - ``is_valid_cnpj``: valida um CNPJ conforme a implementação em ``utils``.

Example:
        >>> extract_number("11987654321")
        ExtractionResult(number='11987654321', success=True)
        >>> is_valid_cnpj("11222333000181")
        True
"""

from teleutils.preprocessing.number_format import (
    extract_number,
)
from teleutils.preprocessing.utils import is_valid_cnpj

__all__ = [
    "extract_number",
    "is_valid_cnpj",
]
