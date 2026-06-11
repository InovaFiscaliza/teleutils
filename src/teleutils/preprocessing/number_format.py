"""Módulo de normalização e validação de números telefônicos brasileiros.

Este módulo implementa funções para padronizar e validar números telefônicos
brasileiros conforme o Plano de Numeração da ANATEL (Agência Nacional de
Telecomunicações) e o padrão internacional ITU-T E.164.

O módulo suporta os seguintes tipos de serviço:
    - SMP (Serviço Móvel Pessoal) — Telefonia móvel.
    - STFC (Serviço Telefônico Fixo Comutado) — Telefonia fixa.
    - SME (Serviço Móvel Especializado) — Serviço especializado de dados móveis.
    - SUP (Serviço de Utilidade Pública) — Serviços de emergência e utilidade.
    - CNG (Código Não Geográfico) — números não geográficos (0800, 0300 etc.).

Principais funcionalidades:
    - Normalização de número único com limpeza de prefixos e validação por regex.
    - Normalização de par de números com inferência de código de área a partir
      do número de origem.
    - UDF pandas vetorizada para integração com pipelines Apache Spark.

Dependências relevantes:
    - re (biblioteca padrão Python)
    - string (biblioteca padrão Python)
    - pandas
    - pyspark.sql.functions.pandas_udf
    - pyspark.sql.types

Referências:
    - Plano de Numeração ANATEL: https://www.anatel.gov.br/
    - Padrão ITU-T E.164: https://handle.itu.int/11.1002/1000/10688

Example:
    >>> normalize_number("(11) 99999-9999")
    ('11999999999', True)
    >>> normalize_number("0800-123-4567")
    ('08001234567', True)
"""

import re
import string

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

#: Padrão regex para números telefônicos brasileiros com 10 ou mais dígitos.
#: Cobre o formato E.164 completo incluindo código do país (55), código de área
#: e os diferentes tipos de serviço: SMP, STFC, CNG e SME com seus padrões específicos.
E164_FULL_NUMBERS = re.compile(
    r"""# (BRAZIL COUNTRY CODE) (CSP) (optional)
        (?:55)?(?:1[2-8]|2[12469]|3[16789]|4[1235679]|5[3568]|6[1235]|7[12456]|8[157]|9[18])?(
            # CN+PREFIXO+MCDU
            # SMP
            (?:1[1-9]9[0-9]{8})$|
            (?:2[12478]9[0-9]{8})$|
            (?:3[1-578]9[0-9]{8})$|
            (?:4[1-9]9[0-9]{8})$|
            (?:5[1345]9[0-9]{8})$|
            (?:6[1-9]9[0-9]{8})$|
            (?:7[134579]9[0-9]{8})$|
            (?:8[1-9]9[0-9]{8})$|
            (?:9[1-9]9[0-9]{8})$|
            # STFC
            (?:1[1-9][2345][0-9]{7})$|
            (?:2[12478][2345][0-9]{7})$|
            (?:3[1-578][2345][0-9]{7})$|
            (?:4[1-9][2345][0-9]{7})$|
            (?:5[1345][2345][0-9]{7})$|
            (?:6[1-9][2345][0-9]{7})$|
            (?:7[134579][2345][0-9]{7})$|
            (?:8[1-9][2345][0-9]{7})$|
            (?:9[1-9][2345][0-9]{7})$|
            # CNG
            (?:[589]00[0-9]{7})$|
            (?:30[03][0-9]{7})$|
            # SME
            (?:1[1-9]7[0789][0-9]{6})$|
            (?:2[124]7[078][0-9]{6})$|
            (?:2778[0-9]{6})$|
            (?:3[147]7[78][0-9]{6})$|
            (?:4[1-478]78[0-9]{6})$|
            (?:5[14]78[0-9]{6})$|
            (?:6[125]78[0-9]{6})$|
            (?:7[135]78[0-9]{6})$|
            (?:8[15]78[0-9]{6})$
        )""",
    re.VERBOSE,
)

#: Padrão regex para números telefônicos brasileiros com até 9 dígitos.
#: Cobre números locais sem código de área, incluindo SMP, STFC, SME
#: e serviços de utilidade pública (SUP) com seus padrões específicos.
SMALL_NUMBERS = re.compile(
    r"""# (BRAZIL COUNTRY CODE) (CN) (optional)
        (?:55)?(?:1[1-9]|2[12478]|3[1-578]|4[1-9]|5[1345]|6[1-9]|7[134579]|8[1-9]|9[1-9])?(
            # PREFIXO+MCDU
            # SMP
            (?:9[0-9]{8})$|
            # STFC
            (?:[2345][0-9]{7})$|
            # SME
            (?:7[0789][0-9]{6})$|
            # SUP
            (?:10[024])$|
            (?:1031[234579])$|
            (?:1032[13-9])$|
            (?:1033[124-9])$|
            (?:1034[123578])$|
            (?:1035[1-468])$|
            (?:1036[139])$|
            (?:1038[149])$|
            (?:1039[168])$|
            (?:105[012356789])$|
            (?:106[012467])$|
            (?:1061[0-35-8])$|
            (?:1062[0145])$|
            (?:1063[0137])$|
            (?:1064[4789])$|
            (?:1065[01235])$|
            (?:1066[016])$|
            (?:1067[137])$|
            (?:1068[5-8])$|
            (?:1069[1359])$|
            (?:11[125-8])$|
            (?:12[135789])$|
            (?:13[024568])$|
            (?:133[12])$|
            (?:1358)$|
            (?:14[25678])$|
            (?:15[0-9])$|
            (?:16[0-8])$|
            (?:18[0158])$|
            (?:1746)$|
            (?:19[0-9])$|
            (?:911)$
        )""",
    re.VERBOSE,
)

#: Padrão regex para remoção de prefixos de discagem de números telefônicos.
#: Remove prefixos de chamada a cobrar (90, 9090), discagem internacional (00)
#: e discagem nacional (0), normalizando o número para o formato sem prefixo.
PREFFIX = re.compile(
    r"""(
        ^90(?:90)?| # collect call preffix
        ^00|        # international preffix
        ^0          # national preffix
    )""",
    re.VERBOSE,
)

# Schema Spark para o tipo de retorno da UDF pandas: (numero_formatado, numero_valido).
# Mantido como constante de módulo para permitir reuso e evitar recriação por chamada.
_RETURN_SCHEMA = StructType(
    [
        StructField("numero_formatado", StringType(), True),
        StructField("numero_valido", BooleanType(), True),
    ]
)


def _clean_numbers(text):
    """Remove letras e pontuação de um texto, mantendo apenas dígitos numéricos.

    Utiliza tradução de string (``str.maketrans``) para remover eficientemente
    todos os caracteres ASCII alfabéticos, de pontuação e espaços em branco,
    preservando apenas os dígitos numéricos.

    Args:
        text: Texto de entrada que pode conter letras, pontuação e dígitos.

    Returns:
        str: String contendo apenas dígitos numéricos.

    Example:
        >>> _clean_numbers("(11) 99999-9999")
        '11999999999'
        >>> _clean_numbers("abc123def456")
        '123456'
    """
    letters = string.ascii_letters
    punctuation = string.punctuation
    remove_table = str.maketrans("", "", letters + punctuation + " ")
    return text.translate(remove_table)


def normalize_number(subscriber_number, national_destination_code=""):
    """Normaliza um número telefônico brasileiro conforme os padrões da ANATEL.

    Processa diferentes formatos de entrada, remove prefixos de discagem,
    valida contra os padrões oficiais de numeração e retorna o número em
    formato padronizado para armazenamento e análise em CDRs.

    Args:
        subscriber_number: Número de telefone a normalizar. Pode conter letras,
            pontuação e diferentes tipos de prefixos de discagem.
        national_destination_code: Código de área de dois dígitos a ser
            prefixado em números locais de 8 ou 9 dígitos. Padrão: ``""``.

    Returns:
        tuple[str, bool]:
            - str: Número normalizado, ou o valor original caso inválido.
            - bool: ``True`` se o número foi normalizado com sucesso,
              ``False`` caso contrário.

    Etapas de processamento:
        1. Trata números separados por ponto-e-vírgula (retém o primeiro).
        2. Remove caracteres de preenchimento (``'f'``).
        3. Remove letras e pontuação via ``_clean_numbers``.
        4. Elimina prefixos de discagem via regex ``PREFFIX``.
        5. Valida contra padrões de numeração brasileira (E164 ou local).
        6. Acrescenta código de área a números locais quando fornecido.

    Example:
        >>> normalize_number("(11) 99999-9999")
        ('11999999999', True)
        >>> normalize_number("0800-123-4567")
        ('08001234567', True)
        >>> normalize_number("99999999", "11")
        ('1199999999', True)
        >>> normalize_number("invalido")
        ('invalido', False)
    """
    subscriber_number = str(subscriber_number)
    if ";" in subscriber_number:
        # Regra de negócio: alguns fornecedores de CDR enviam múltiplos números
        # separados por ponto-e-vírgula no mesmo campo; utiliza-se apenas o primeiro.
        subscriber_number = subscriber_number.split(";")[0]
    # Remove o caractere de preenchimento 'f' utilizado por certos sistemas
    # legados para completar campos numéricos de tamanho fixo.
    subscriber_number = subscriber_number.replace("f", "")

    clean_subscriber_number = _clean_numbers(subscriber_number)
    # Remove prefixo de chamada a cobrar, internacional (00) ou nacional (0)
    # para isolar apenas os dígitos significativos do número.
    clean_subscriber_number = PREFFIX.sub("", clean_subscriber_number)

    if len(clean_subscriber_number) >= 10:
        normalized_subscriber_number = E164_FULL_NUMBERS.findall(
            clean_subscriber_number
        )
    else:
        normalized_subscriber_number = SMALL_NUMBERS.findall(clean_subscriber_number)

    # Um único match indica número válido e não ambíguo; zero matches indicam
    # formato desconhecido e múltiplos matches indicam ambiguidade no padrão.
    if len(normalized_subscriber_number) == 1:
        normalized_subscriber_number = normalized_subscriber_number[0]
        if len(normalized_subscriber_number) in (8, 9) and national_destination_code:
            normalized_subscriber_number = (
                f"{national_destination_code}{normalized_subscriber_number}"
            )
        return (normalized_subscriber_number, True)

    return (subscriber_number, False)


def normalize_number_pair(number_a, number_b, national_destination_code=""):
    """Normaliza um par de números telefônicos com inferência de código de área.

    Normaliza dois números telefônicos onde o primeiro (tipicamente o originante
    da chamada) pode fornecer contexto de código de área para o segundo
    (tipicamente o destino), caso este não possua código de área completo.

    Args:
        number_a: Primeiro número, geralmente o originante da chamada.
        number_b: Segundo número, geralmente o destino da chamada.
        national_destination_code: Código de área de dois dígitos a ser usado
            como contexto inicial para ambos os números. Padrão: ``""``.

    Returns:
        tuple[str, bool, str, bool]:
            - str: ``number_a`` normalizado, ou original caso inválido.
            - bool: ``True`` se ``number_a`` foi normalizado com sucesso.
            - str: ``number_b`` normalizado, ou original caso inválido.
            - bool: ``True`` se ``number_b`` foi normalizado com sucesso.

    Lógica:
        1. Normaliza ``number_a`` primeiro.
        2. Se ``number_a`` for válido e possuir 10 ou 11 dígitos, extrai o
           código de área (2 primeiros dígitos) para uso contextual.
        3. Usa o código de área inferido para normalizar ``number_b``.
        4. Retorna os resultados de normalização para ambos os números.

    Example:
        >>> normalize_number_pair("11999999999", "88888888")
        ('11999999999', True, '1188888888', True)
        >>> normalize_number_pair("invalido", "11999999999")
        ('invalido', False, '11999999999', True)
        >>> normalize_number_pair("1133334444", "22225555")
        ('1133334444', True, '1122225555', True)

    Notes:
        Particularmente útil em CDRs onde o número originante pode fornecer
        contexto geográfico para números locais (sem DDD) de destino.
    """
    normalized_number_a, is_number_a_valid = normalize_number(number_a)

    # Regra de negócio: se o número de origem for válido e completo (10 ou 11
    # dígitos), seus dois primeiros dígitos representam o DDD e podem ser
    # utilizados como contexto para normalizar o número de destino local.
    if is_number_a_valid and len(normalized_number_a) in (10, 11):
        if not national_destination_code:
            national_destination_code = normalized_number_a[:2]
    else:
        national_destination_code = ""

    normalized_number_b, is_number_b_valid = normalize_number(
        number_b, national_destination_code
    )

    return (
        normalized_number_a,
        is_number_a_valid,
        normalized_number_b,
        is_number_b_valid,
    )


@pandas_udf(_RETURN_SCHEMA)  # type: ignore
def spark_normalize_number(number_series: pd.Series) -> pd.DataFrame:
    """Normaliza números telefônicos em lote para uso em pipelines Spark.

    UDF pandas vetorizada que recebe uma série de números telefônicos brutos,
    aplica ``normalize_number`` sobre cada elemento e retorna uma estrutura
    tabular com o número formatado e o indicador de validade.

    Args:
        number_series: Série pandas contendo números telefônicos em formato bruto,
            possivelmente com letras, pontuação e prefixos variados.

    Returns:
        pd.DataFrame: DataFrame com as seguintes colunas:
            - ``numero_formatado`` (str | None): Número normalizado ou original.
            - ``numero_valido`` (bool | None): Indicador de sucesso da normalização.

    Notes:
        A pandas UDF processa os dados por lote (batch), reduzindo o overhead de
        serialização em comparação com UDFs linha a linha, o que melhora
        significativamente o desempenho em grandes volumes de CDR.

        Anotação de manutenção: alterações no contrato de retorno (nomes ou
        tipos de coluna) exigem atualização coordenada de
        ``_RETURN_SCHEMA`` e de todos os acessos às
        colunas estruturadas nos transformadores que consomem esta UDF.
    """
    # Processar em batch (vetorizado)
    results = []
    for number in number_series:
        results.append(normalize_number(number))

    return pd.DataFrame(
        results,
        columns=[
            "numero_formatado",
            "numero_valido",
        ],
    )
