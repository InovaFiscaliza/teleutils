"""Pacote de pré-processamento: normalização e validação de dados.

Reexporta as funções de normalização de números telefônicos brasileiros
(``normalize_number``, ``normalize_number_pair``, ``spark_normalize_number``) e
de validação de CNPJ (``validar_cnpj``, ``spark_validar_cnpj``) utilizadas
pelas camadas de transformação do projeto.
"""

from teleutils.preprocessing.number_format import (
    normalize_number,
    normalize_number_pair,
    spark_normalize_number,
)
from teleutils.preprocessing.utils import spark_validar_cnpj, validar_cnpj

__all__ = [
    "normalize_number",
    "normalize_number_pair",
    "spark_normalize_number",
    "spark_validar_cnpj",
    "validar_cnpj",
]
