"""Validação local de CNPJs numéricos e alfanuméricos.

O módulo fornece ``is_valid_cnpj``, função que sanitiza a entrada, verifica a
estrutura de 14 caracteres e confere os dois dígitos verificadores pelo
algoritmo de módulo 11. Ela aceita o formato numérico tradicional e o formato
alfanumérico com 12 caracteres de base e dois DVs numéricos. A função também é
reexportada pelo pacote ``teleutils.preprocessing``.

Dependências:
    - ``re`` (biblioteca padrão do Python), para sanitização e validação do
      formato.

Examples:
    >>> is_valid_cnpj("11.222.333/0001-81")
    True
    >>> is_valid_cnpj("12ABC34501DE35")
    True
"""

from __future__ import annotations

import re


def is_valid_cnpj(cnpj: str | int) -> bool:
    """Valida um CNPJ numérico ou alfanumérico pelas regras oficiais de DV.

    O CNPJ alfanumérico tem 12 caracteres (``0-9`` e ``A-Z``) seguidos de
    2 dígitos verificadores numéricos. Os DVs são calculados por módulo 11,
    com pesos de 2 a 9 aplicados da direita para a esquerda (reiniciando
    após o 8º caractere). Cada caractere vale ``ord(c) - 48``, o que mantém
    os dígitos com o próprio valor (0-9) e dá às letras A=17, B=18, ..., Z=42.
    O CNPJ numérico é um caso particular desse algoritmo.

    Args:
        cnpj: CNPJ como texto (com ou sem máscara) ou inteiro.

    Returns:
        True se o formato e os dígitos verificadores forem válidos;
        False caso contrário.

    Notes:
        Sanitização aplicada antes da validação:
            - ``None`` e booleanos são rejeitados.
            - Remoção de tudo que não seja ``0-9`` ou ``A-Z``/``a-z``
              (pontos, barra, hífen, espaços etc.).
            - Letras minúsculas são convertidas para maiúsculas.
            - Entradas totalmente numéricas com menos de 14 dígitos são
              preenchidas com zeros à esquerda (útil para ``int``).
              Entradas com letras precisam ter exatamente 14 caracteres.

        Os dois últimos caracteres (DVs) devem ser numéricos. Sequências de
        14 caracteres idênticos são rejeitadas.
    """
    # 1. Sanitização e validações defensivas de entrada.
    # bool é subtipo de int em Python (True -> "1"), então é rejeitado antes.
    if cnpj is None or isinstance(cnpj, bool):
        return False

    try:
        valor = str(cnpj)
    except (TypeError, ValueError):
        return False

    # Mantém só ASCII alfanumérico e normaliza para maiúsculas.
    valor = re.sub(r"[^0-9A-Za-z]", "", valor).upper()

    if not valor:
        return False

    # Entradas puramente numéricas (ex.: int) podem ter perdido zeros à esquerda.
    if valor.isdigit():
        valor = valor.zfill(14)

    # 2. Estrutura: 12 caracteres alfanuméricos + 2 dígitos verificadores.
    if not re.fullmatch(r"[0-9A-Z]{12}[0-9]{2}", valor):
        return False

    # Rejeita sequências formadas pelo mesmo caractere.
    if len(set(valor)) == 1:
        return False

    def calcular_digito(base: str) -> int:
        """Calcula um DV (módulo 11) para a base informada."""
        # Pesos 2..9 da direita para a esquerda, reiniciando após o 9.
        soma = sum((ord(c) - 48) * (2 + i % 8) for i, c in enumerate(reversed(base)))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    # 3. Cálculo dos DVs (o segundo inclui o primeiro na base).
    digito_1 = calcular_digito(valor[:12])
    digito_2 = calcular_digito(valor[:12] + str(digito_1))

    # 4. Verificação final.
    return valor[12:] == f"{digito_1}{digito_2}"
