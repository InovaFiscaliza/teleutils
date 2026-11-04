"""Extração e validação de números telefônicos em entradas textuais.

O módulo define expressões regulares para números STFC, SME, SMP, CNG e
números de serviços, e usa esses padrões para localizar e classificar um
número em uma entrada. ``extract_number`` retorna o número encontrado e
um indicador de sucesso. Quando a entrada é vazia (falsy), o número retornado
é ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``, definido em ``teleutils._config``.
Quando a entrada não é vazia e nenhuma extração é válida, o valor de entrada
original é devolvido sem alteração, com ``success=False``.

O processamento converte a entrada não vazia em texto minúsculo, remove um
``f`` final e aplica ``CLEAN_PATTERN``. Em seguida, tenta o padrão associado
ao tamanho do texto limpo; se não houver extração, pesquisa padrões de fallback
em uma ordem definida. Números que correspondam ao padrão de rejeição não são
retornados como extrações válidas.

Os padrões incluem regras de formato comentadas no próprio código, algumas
referenciadas à Resolução 749 da ANATEL. A validação corresponde às expressões
implementadas neste módulo e não realiza consulta externa.

Dependências relevantes:
    - ``re``: compilação e busca de expressões regulares.
    - ``typing.NamedTuple``: estrutura do resultado de ``extract_number``.

Example:
    >>> extract_number("987654321")
    ExtractionResult(number='987654321', success=True)
"""

import re
from typing import NamedTuple

from teleutils._config import SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE


class ExtractionResult(NamedTuple):
    """Resultado da tentativa de extração de um número.

    Attributes:
        number: Número extraído. Quando a entrada é vazia, é
            ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``; quando a extração falha
            para uma entrada não vazia, é o valor de entrada original.
        success: Indica se um número foi extraído e não foi rejeitado pelo
            padrão de números repetidos.
    """

    number: str
    success: bool


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

# Serviços de 3 dígitos (ex.: 190), sem CN; compõem _SERVICES_PATTERN.
_3_DIGIT_SERVICES_PATTERN = r"""
    (?:
        10[02-6]|
        11[125-8]|
        12[1357-9]|
        13[024-68]|
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
        4[3789]|
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

# group(1): apenas o número do serviço, sem o CN
# padrão de números de serviços de utilidade pública deve ser mais
# restritivo que de números fixos ou móveis para minimizar falsos positivos.
UTILITY_NUMBERS_PATTERN = re.compile(
    rf"""
    ^
    (?:0?{_CN_PATTERN})?
    ({_SERVICES_PATTERN})
    $
    """,
    re.VERBOSE,
)

# Resolução 749, art. 11
_STFC_PATTERN = r"[2-6][0-9]{7}"

# Resolução 749 não menciona SME
# Na base da EASI há apenas prefixo com N8=7 designado
_SME_PATTERN = r"7[0-9]{7}"

# Resolução 749, art. 12
# Resolução destina número 7,8 e 9 para N9
# Na base da EASI há apenas N9=9 atribuídos
_SMP_PATTERN = r"9[0-9]{8}"

# Resolução 749, art. 18 (Exceção)
# Ainda há alguns 0800 com 9 dígitos
_SHORT_CNG_PATTERN = r"(?:800[0-9]{6})"

_LONG_CNG_PATTERN = r"""
    (?:
        [3589]00[0-9]{7}|
        303[0-9]{7}
    )
"""

# group(1): CN + fixo/móvel, ou CNG
_NUMBERS_PATTERN = f"(?:{_SMP_PATTERN}|{_SME_PATTERN}|{_STFC_PATTERN})"
_ALL_CNG_PATTERN = f"(?:{_LONG_CNG_PATTERN}|{_SHORT_CNG_PATTERN})"

LARGE_NUMBERS_PATTERN = re.compile(
    rf"^((?:{_CN_PATTERN}){_NUMBERS_PATTERN}|{_ALL_CNG_PATTERN})$",
    re.VERBOSE,
)

# group(1): fixo ou móvel (8 ou 9 dígitos), sem CN ou CNG curto
MID_NUMBERS_PATTERN = re.compile(
    rf"^({_NUMBERS_PATTERN}|{_SHORT_CNG_PATTERN})$",
    re.VERBOSE,
)


# Padrões de fallback para extrair números de várias posições na entrada, considerando prioridades:
# 1 números móveis (SMP ou SME)
# 2 números fixos (STFC)
# 3 números de CNG (curto ou longo)

_MOBILE_BODY = f"(?:{_CN_PATTERN})(?:{_SMP_PATTERN}|{_SME_PATTERN})"
_LANDLINE_BODY = f"(?:{_CN_PATTERN}){_STFC_PATTERN}"
_CNG_BODY = f"(?:{_LONG_CNG_PATTERN}|{_SHORT_CNG_PATTERN})"


def _build_patterns(body: str) -> tuple[re.Pattern, re.Pattern, re.Pattern]:
    """Compila buscas ancoradas no fim, no início e em qualquer posição.

    Args:
        body: Expressão regular que representa o número a localizar.

    Returns:
        Tupla de três padrões compilados, cada um com um grupo de captura:
        busca no fim da entrada, no início e em qualquer posição,
        respectivamente.
    """
    return (
        re.compile(rf"({body})$", re.VERBOSE),  # fim
        re.compile(rf"^({body})", re.VERBOSE),  # início
        re.compile(rf"({body})", re.VERBOSE),  # qualquer ponto
    )


TRAILING_MOBILE_PATTERNS, LEADING_MOBILE_PATTERNS, ANYWHERE_MOBILE_PATTERNS = (
    _build_patterns(_MOBILE_BODY)
)
TRAILING_LANDLINE_PATTERNS, LEADING_LANDLINE_PATTERNS, ANYWHERE_LANDLINE_PATTERNS = (
    _build_patterns(_LANDLINE_BODY)
)
TRAILING_CNG_PATTERNS, LEADING_CNG_PATTERNS, ANYWHERE_CNG_PATTERNS = _build_patterns(
    _CNG_BODY
)

# group(1): fixo ou móvel (8 ou 9 dígitos), sem CN, após o prefixo 9090
COLLECT_CALL_PATTERN = re.compile(
    rf"9090({_NUMBERS_PATTERN})",
    re.VERBOSE,
)

FALLBACK_PATTERNS = [
    TRAILING_MOBILE_PATTERNS,
    LEADING_MOBILE_PATTERNS,
    TRAILING_LANDLINE_PATTERNS,
    LEADING_LANDLINE_PATTERNS,
    TRAILING_CNG_PATTERNS,
    LEADING_CNG_PATTERNS,
    COLLECT_CALL_PATTERN,
    ANYWHERE_MOBILE_PATTERNS,
    ANYWHERE_LANDLINE_PATTERNS,
    ANYWHERE_CNG_PATTERNS,
]

# Remove zeros iniciais e, separadamente, sufixos de oito ou mais dígitos iguais.
CLEAN_PATTERN = re.compile(r"^0+|(\d)\1{7,}$")


# Padrão para números rejeitados: todos os dígitos iguais ou duas sequências de dígitos repetidos.
REJECTED_NUMBERS_PATTERN = re.compile(
    rf"^(?:{_CN_PATTERN})?(?:(\d)\1+|(\d)\2*(?!\2)(\d)\3*)$",
    re.VERBOSE,
)


def _is_rejected_number(subscriber_number):
    """Indica se o texto corresponde ao padrão de números repetidos rejeitados.

    Args:
        subscriber_number: Texto comparado com ``REJECTED_NUMBERS_PATTERN``.

    Returns:
        ``True`` quando o padrão encontra correspondência; caso contrário,
        ``False``.
    """
    return REJECTED_NUMBERS_PATTERN.match(subscriber_number) is not None


def _extract_number(pattern, subscriber_number):
    """Obtém o grupo final da primeira correspondência se ele não for rejeitado.

    Args:
        pattern: Padrão compilado usado para pesquisar a entrada.
        subscriber_number: Texto no qual a busca será realizada.

    Returns:
        Texto capturado pelo último grupo do padrão, ou ``None`` quando não há
        correspondência ou o texto capturado é rejeitado por
        ``REJECTED_NUMBERS_PATTERN``.
    """
    if match := pattern.search(subscriber_number):
        if _is_rejected_number(match.group(match.lastindex)):
            return None
        return match.group(match.lastindex)
    return None


def _search_number(subscriber_number, fallback_patterns):
    """Tenta os padrões de fallback na ordem recebida.

    Args:
        subscriber_number: Texto limpo no qual os padrões serão pesquisados.
        fallback_patterns: Padrões compilados cuja ordem determina a prioridade
            das tentativas.

    Returns:
        A primeira extração válida encontrada ou ``None`` se nenhuma tentativa
        produzir um número aceito.
    """
    for pattern in fallback_patterns:
        if extracted_number := _extract_number(pattern, subscriber_number):
            return extracted_number
    return None


# (tamanhos aceitos do texto limpo, padrão da tentativa inicial)
_LENGTH_RULES = (
    (range(3, 8), UTILITY_NUMBERS_PATTERN),
    (range(8, 10), MID_NUMBERS_PATTERN),
    (range(10, 12), LARGE_NUMBERS_PATTERN),
)


def extract_number(subscriber_number) -> ExtractionResult:
    """Extrai um número reconhecido de uma entrada e indica se houve sucesso.

    Args:
        subscriber_number: Valor de entrada. Valores falsy são tratados como
            entrada vazia; os demais são convertidos para texto antes da
            limpeza e da busca.

    Returns:
        ExtractionResult: Tupla nomeada com o número extraído e o indicador de
        sucesso. Para uma entrada falsy, retorna
        ``(SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE, False)``. Quando nenhuma
        extração é válida, retorna ``(subscriber_number, False)``, com o valor
        de entrada original, sem limpeza nem conversão para texto.

    Notes:
        O texto é convertido para minúsculas, tem um ``f`` final removido e
        passa por ``CLEAN_PATTERN`` antes da seleção dos padrões. A tentativa
        inicial depende do comprimento resultante: serviços para comprimentos
        de 3 a 7, padrões intermediários para 8 ou 9 e padrões longos para 10
        ou 11. Se essa tentativa não produzir um número, os padrões de fallback
        são pesquisados na ordem de ``FALLBACK_PATTERNS``. A limpeza não remove
        espaços nem pontuação em geral.
    """
    if not subscriber_number:
        return ExtractionResult(SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE, False)

    clean_number = CLEAN_PATTERN.sub(
        "", str(subscriber_number).lower().removesuffix("f")
    )
    length = len(clean_number)

    extracted_number = None

    for lengths, pattern in _LENGTH_RULES:
        if length in lengths:
            extracted_number = _extract_number(pattern, clean_number)
            break

    if not extracted_number:
        extracted_number = _search_number(clean_number, FALLBACK_PATTERNS)

    if extracted_number and not _is_rejected_number(extracted_number):
        return ExtractionResult(extracted_number, True)

    return ExtractionResult(subscriber_number, False)
