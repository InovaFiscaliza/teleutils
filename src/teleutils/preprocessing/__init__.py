"""API pública do pacote de pré-processamento.

Este inicializador reexporta as funções listadas em ``__all__`` para acesso
direto pelo namespace ``teleutils.preprocessing``:

        - ``extract_number``: extrai um número telefônico brasileiro reconhecido
          pelos padrões implementados em ``phone_number_extractor``. Retorna
          um ``ExtractionResult`` com o número e um indicador de sucesso.
          Quando a extração falha, o número retornado é o próprio valor de
          entrada; para entrada vazia ou nula, é o valor sentinela
          ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``.
        - ``is_valid_cnpj``: valida um CNPJ (numérico ou alfanumérico) conforme
          a implementação em ``utils``.

Example:
    >>> extract_number("11987654321")
    ExtractionResult(number='11987654321', success=True)
    >>> is_valid_cnpj("11222333000181")
    True
"""

from teleutils.preprocessing.phone_number_extractor import (
    extract_number,
)
from teleutils.preprocessing.utils import is_valid_cnpj

__all__ = [
    "extract_number",
    "is_valid_cnpj",
]
