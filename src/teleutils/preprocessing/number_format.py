"""Módulo de normalização e validação de números telefônicos brasileiros.

Este módulo implementa a função `normalize_number`, que padroniza e valida
números telefônicos brasileiros por meio de expressões regulares baseadas na
Resolução 749 da ANATEL (referências indicadas nos comentários de cada padrão).

Padrões de numeração reconhecidos:
    - STFC (telefonia fixa): 8 dígitos iniciados em 2 a 6.
    - SMP (telefonia móvel): 9 dígitos iniciados em 7, 8 ou 9.
    - CNG (não geográficos): 10 dígitos iniciados em 300, 303, 500, 800 ou 900
      (ex.: 0800 sem o ``0`` inicial).
    - Serviços de 3, 4 e 5 dígitos (ex.: 190), com CN opcional.

Principais funcionalidades:
    - Remoção seletiva de caracteres ``*`` e ``#`` conforme ``CLEAN_PATTERN``.
    - Remoção de um prefixo de discagem (``90``, ``9090``, ``00`` ou ``0``).
    - Escolha do padrão de validação conforme o tamanho do número limpo.
    - Acréscimo opcional do CN (código nacional de destino, o DDD) a números de
      8 ou 9 dígitos.

Fluxo de processamento:
    Entrada
      ↓
    Valor vazio? → retorna ``SUBSCRIBER_NULL_SENTINEL``
      ↓
    ``str`` + minúsculas + remoção de um ``f`` final
      ↓
    Limpeza seletiva de ``*`` e ``#`` (``CLEAN_PATTERN``)
      ↓
    Remoção do prefixo de discagem (``PREFFIX_PATTERN``)
      ↓
    Seleção do padrão pelo tamanho (>= 10, 8 ou 9, 3 a 5)
      ↓
    Resultado: ``(número_normalizado, True)`` ou ``(entrada_original, False)``

Dependências relevantes:
    - re (biblioteca padrão Python)

Notes:
    - A limpeza de ``*`` e ``#`` é seletiva: remove um caractere inicial se
      houver pelo menos cinco caracteres após ele, remove sequências finais
      desses caracteres e remove ``#9090`` onde aparecer. Espaços, parênteses
      e hífens não são removidos.
    - ``teleutils.preprocessing`` reexporta ``normalize_number`` deste módulo.

Referências:
    - Plano de Numeração da ANATEL: https://www.anatel.gov.br/
    - Padrão ITU-T E.164: https://handle.itu.int/11.1002/1000/10688

Example:
    >>> normalize_number("11999999999")
    ('11999999999', True)
    >>> normalize_number("5511999999999")
    ('11999999999', True)
    >>> normalize_number("08001234567")
    ('8001234567', True)
"""

import re

# Retornado por normalize_number para entradas vazias ou nulas; marca o
# resultado como inválido (False) com um número fixo.
SUBSCRIBER_NULL_SENTINEL = ("5599999999999", False)


#: Resolução 749, art. 15
_CN_PATTERN = r"""
    (?:
        1[1-9]|
        2[12478]|
        3[1-578]|
        4[1-9]|
        5[1345]|
        6[1-9]|
        7[134579]|
        8[1-9]|
        9[1-9]
    )
"""

# Resolução 749, art. 11
_STFC_PATTERN = r"[2-6][0-9]{7}"

# Resolução 749, art. 12
_SMP_PATTERN = r"[789][0-9]{8}"

# Resolução 749, art. 18
_CNG_PATTERN = r"""
    (?:
        [3589]00[0-9]{7}|
        303[0-9]{7}
    )
"""

# Serviços de 3 dígitos (ex.: 190), sem CN; compõem _SERVICES_PATTERN.
_3_DIGIT_SERVICES_PATTERN = r"""
    (?:
        10[02-6]|
        11[125-8]|
        12[1357-9]|
        13[02-68]|
        14[25-8]|
        15[0-9]|
        16[0-8]|
        18[0158]|
        19[0-9]|
        911
    )
"""

_4_DIGIT_SERVICES_PATTERN = r"""
    (?:
        105[0-35-9]|
        106[013467]|
        133[12]|
        1358|
        1746
    )
"""

_103_SERVICES_PATTERN = r"""
    103(?:
        1[2-579]|
        2[13-9]|
        3[124-9]|
        4[1-3578]|
        5[1-468]|
        6[139]|
        8[149]|
        9[168]
    )
"""

_106_SERVICES_PATTERN = r"""
    106(?:
        1[0-35-8]|
        2[0145]|
        3[0137]|
        4[37-9]|
        5[0-35]|
        6[016]|
        7[137]|
        8[5-8]|
        9[1359]
    )
"""

_SERVICES_PATTERN = rf"""
    (?:
        {_3_DIGIT_SERVICES_PATTERN}|
        {_4_DIGIT_SERVICES_PATTERN}|
        {_103_SERVICES_PATTERN}|
        {_106_SERVICES_PATTERN}
    )
"""


# group(1): CN + fixo/móvel, ou CNG, sempre com o CN quando houver.
# Não possui âncora ``^``: com ``search``, aceita qualquer prefixo antes do
# trecho final (ex.: ``55`` + CN + número), que é descartado do resultado.
LARGE_NUMBERS_PATTERN = re.compile(
    rf"""
    (
        {_CN_PATTERN}
        (?:
            {_STFC_PATTERN}|
            {_SMP_PATTERN}
        )|
        {_CNG_PATTERN}
    )$
    """,
    re.VERBOSE,
)

# group(1): fixo ou móvel (8 ou 9 dígitos), sem CN
MID_NUMBERS_PATTERN = re.compile(
    rf"""
    ^(
        {_STFC_PATTERN}|
        {_SMP_PATTERN}
    )
    $
    """,
    re.VERBOSE,
)

# group(1): apenas o número do serviço, sem o CN
UTILITY_NUMBERS_PATTERN = re.compile(
    rf"""
    ^
    (?:{_CN_PATTERN})?
    ({_SERVICES_PATTERN})
    $
    """,
    re.VERBOSE,
)

# Remove marcadores de discagem apenas nos contextos especificados pelas
# alternativas; não realiza limpeza geral de pontuação ou espaços.
CLEAN_PATTERN = re.compile(
    r"""
        ^[*#](?=.{5})|    # "*" ou "#" inicial, só se a string tiver mais de 5 caracteres
        [*#]+$|           # um ou mais "*" ou "#" no final
        \#9090            # prefixo de chamada a cobrar (#9090) no meio do número
    """,
    re.VERBOSE,
)

PREFFIX_PATTERN = re.compile(
    r"""
    ^(?:
        90(?:90)?|  # prefixo de chamada a cobrar
        00|         # prefixo de discagem internacional
        0           # prefixo de discagem nacional
    )
    """,
    re.VERBOSE,
)


def normalize_number(subscriber_number, national_destination_code=""):
    """Normaliza um número telefônico brasileiro conforme os padrões da ANATEL.

    Converte a entrada em texto, remove um prefixo de discagem, escolhe o
    padrão de validação pelo tamanho do número restante e retorna o número
    normalizado com um indicador de validade.

    Args:
        subscriber_number: Número a normalizar. É convertido com ``str``; deve
            conter dígitos; um ``f`` final é removido após conversão para
            minúsculas. ``CLEAN_PATTERN`` remove ``*`` e ``#`` somente em
            posições específicas; pontuação comum, espaços e outras letras
            não são removidos.
        national_destination_code: CN (DDD) a ser prefixado somente quando o
            número limpo tiver 8 ou 9 dígitos e for válido. Padrão: ``""``
            (nenhum acréscimo).

    Returns:
        tuple[str, bool]:
            - Entrada vazia ou nula: ``SUBSCRIBER_NULL_SENTINEL``
              (``("5599999999999", False)``).
            - Número válido: ``(número_normalizado, True)``.
            - Número inválido: ``(subscriber_number, False)``, com o valor
              original sem conversão ou limpeza.

    Notes:
        Etapas de processamento:
            1. Entradas falsy (``None``, ``""``) retornam o sentinela.
            2. A entrada é convertida para minúsculas e perde um ``f`` final.
            3. ``CLEAN_PATTERN`` remove ``*``/``#`` inicial quando seguido de
               pelo menos cinco caracteres, sequências finais desses
               caracteres e ocorrências de ``#9090``.
            4. ``PREFFIX_PATTERN`` remove um prefixo de discagem inicial.
            5. O tamanho do resultado define o padrão: 10 ou mais dígitos
               (``LARGE_NUMBERS_PATTERN``), 8 ou 9 (``MID_NUMBERS_PATTERN``),
               3 a 5 (``UTILITY_NUMBERS_PATTERN``); outros tamanhos são
               inválidos.
            6. O ``group(1)`` do padrão é o número normalizado: sem prefixo
               de discagem, sem código de país (``55``) e, para serviços, sem
               CN. O 0 inicial de 0800/0300 é removido como prefixo
               nacional, e não é restaurado.

        O bloco ``except IndexError`` retorna ``SUBSCRIBER_NULL_SENTINEL``;
        os padrões usados definem ``group(1)``, portanto esse retorno é
        defensivo.

    Example:
        >>> normalize_number("11999999999")
        ('11999999999', True)
        >>> normalize_number("5511999999999")
        ('11999999999', True)
        >>> normalize_number("08001234567")
        ('8001234567', True)
        >>> normalize_number("190")
        ('190', True)
        >>> normalize_number("invalido")
        ('invalido', False)
    """

    if not subscriber_number:
        return SUBSCRIBER_NULL_SENTINEL

    # Converte para minúsculas e remove no máximo um 'f' final (comparação
    # case-insensitive com o sufixo).
    clean_subscriber_number = str(subscriber_number).lower().removesuffix("f")

    # Remove caracteres indesejados usando CLEAN_PATTERN
    clean_subscriber_number = CLEAN_PATTERN.sub("", clean_subscriber_number)

    # Substitui o prefixo encontrado por uma string vazia ""
    clean_subscriber_number = PREFFIX_PATTERN.sub("", clean_subscriber_number)

    len_clean_subscriber_number = len(clean_subscriber_number)

    if len_clean_subscriber_number >= 10:
        pattern = LARGE_NUMBERS_PATTERN
    elif len_clean_subscriber_number in (8, 9):
        pattern = MID_NUMBERS_PATTERN
    elif len_clean_subscriber_number in (3, 4, 5):
        pattern = UTILITY_NUMBERS_PATTERN
    else:
        return (subscriber_number, False)

    normalized_subscriber_number = pattern.search(clean_subscriber_number)
    if normalized_subscriber_number:
        try:
            if len_clean_subscriber_number in (8, 9) and national_destination_code:
                return (
                    national_destination_code + normalized_subscriber_number.group(1),
                    True,
                )
            return (normalized_subscriber_number.group(1), True)
        except IndexError:
            return SUBSCRIBER_NULL_SENTINEL

    return (subscriber_number, False)
