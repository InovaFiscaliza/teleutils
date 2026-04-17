"""
Brazilian Phone Number Normalization Module.

This module provides functionality for normalizing and validating Brazilian phone numbers
according to ANATEL (Brazilian National Telecommunications Agency) numbering plans and
ITU-T E.164 international standard.

The module handles various Brazilian number formats including:
- SMP (Serviço Móvel Pessoal) - Mobile services
- STFC (Serviço Telefônico Fixo Comutado) - Fixed-line services
- SME (Serviço Móvel Especializado) - Specialized mobile services
- SUP (Serviço de Utilidade Pública) - Public utility services
- CNG (Código Nacional de Gratuidade) - National free-call codes

Functions:
    normalize_number: Normalizes a single Brazilian phone number.
    normalize_number_pair: Normalizes a pair of related phone numbers with context.

Private Functions:
    _clean_numbers: Removes letters and punctuation from number strings.

Constants:
    E164_FULL_NUMBERS: Regex pattern for numbers with length >= 10 digits.
    SMALL_NUMBERS: Regex pattern for numbers with length <= 9 digits.
    PREFFIX: Regex pattern for removing call prefixes.

Example:
    >>> normalize_number("(11) 99999-9999")
    ['11999999999', True]
    >>> normalize_number("0800-123-4567")
    ['08001234567', True]

References:
    - ANATEL Numbering Plan: https://www.anatel.gov.br/
    - ITU-T E.164 Standard: https://handle.itu.int/11.1002/1000/10688

"""

import re
import string

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

#: Regex pattern for matching Brazilian phone numbers with length >= 10 digits.
#: Covers full E.164 format numbers including country code (55), area codes,
#: and various service types (SMP, STFC, CNG, SME) with their specific patterns.
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

#: Regex pattern for matching Brazilian phone numbers with length <= 9 digits.
#: Covers local numbers without area codes including mobile (SMP), fixed-line (STFC),
#: specialized mobile (SME), and public utility services (SUP) patterns.
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

#: Regex pattern for removing call prefixes from Brazilian phone numbers.
#: Removes collect call prefixes (90, 9090), international prefix (00),
#: and national long-distance prefix (0) to normalize numbers.
PREFFIX = re.compile(
    r"""(
        ^90(?:90)?| # collect call preffix
        ^00|        # international preffix
        ^0          # national preffix
    )""",
    re.VERBOSE,
)

# Schema definition for Spark UDF return type (numero_formatado, numero_valido)
SPARK_NORMALIZE_NUMBER_RETURN_SCHEMA = StructType(
    [
        StructField("numero_formatado", StringType(), True),
        StructField("numero_valido", BooleanType(), True),
    ]
)


def _clean_numbers(text):
    """
    Remove letters and punctuation from a text string, keeping only digits.

    This private function uses string translation to efficiently remove all
    ASCII letters and punctuation characters, leaving only numeric digits.

    Args:
        text (str): Input string that may contain letters, punctuation, and digits.

    Returns:
        str: String containing only numeric digits.

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
    """
    Normalize a Brazilian phone number according to ANATEL standards.

    This function processes various formats of Brazilian phone numbers, removes
    prefixes, validates against official numbering patterns, and returns a
    normalized format suitable for database storage and analysis.

    Args:
        subscriber_number (str or int): The phone number to normalize. Can contain
            letters, punctuation, and various prefixes.
        national_destination_code (str, optional): Two-digit area code to prepend
            to 8-9 digit local numbers. Defaults to "".

    Returns:
        tuple: A two-element tuple containing:
            - str: The normalized phone number (or original if invalid)
            - bool: True if number was successfully normalized, False otherwise

    Processing Steps:
        1. Handles semicolon-separated numbers (takes first part)
        2. Removes filler characters ('f')
        3. Removes letters and punctuation
        4. Strips call prefixes (collect call, international, national)
        5. Validates against Brazilian numbering patterns
        6. Adds area code to local numbers when provided

    Examples:
        >>> normalize_number("(11) 99999-9999")
        ('11999999999', True)
        >>> normalize_number("0800-123-4567")
        ('08001234567', True)
        >>> normalize_number("99999999", "11")
        ('1199999999', True)
        >>> normalize_number("invalid")
        ('invalid', False)

    """
    subscriber_number = str(subscriber_number)
    if ";" in subscriber_number:
        subscriber_number = subscriber_number.split(";")[0]
    # remove filler
    subscriber_number = subscriber_number.replace("f", "")

    clean_subscriber_number = _clean_numbers(subscriber_number)
    # remove collect call indicator or the international/national prefix
    clean_subscriber_number = PREFFIX.sub("", clean_subscriber_number)

    if len(clean_subscriber_number) >= 10:
        normalized_subscriber_number = E164_FULL_NUMBERS.findall(
            clean_subscriber_number
        )
    else:
        normalized_subscriber_number = SMALL_NUMBERS.findall(clean_subscriber_number)

    # if not exactly one match
    if len(normalized_subscriber_number) == 1:
        normalized_subscriber_number = normalized_subscriber_number[0]
        if len(normalized_subscriber_number) in (8, 9) and national_destination_code:
            normalized_subscriber_number = (
                f"{national_destination_code}{normalized_subscriber_number}"
            )
        return (normalized_subscriber_number, True)

    return (subscriber_number, False)


def normalize_number_pair(number_a, number_b, national_destination_code=""):
    """
    Normalize a pair of related Brazilian phone numbers with contextual area code inference.

    This function normalizes two phone numbers where the first number (typically
    the calling number) can provide area code context for the second number
    (typically the called number) if it lacks an area code.

    Args:
        number_a (str or int): First phone number, often the calling/originating number.
        number_b (str or int): Second phone number, often the called/destination number.
        national_destination_code (str, optional): Two-digit area code to prepend
            to 8-9 digit local numbers. Defaults to "".

    Returns:
        tuple: A four-element tuple containing:
            - str: Normalized number_a (or original if invalid)
            - bool: True if number_a was successfully normalized
            - str: Normalized number_b (or original if invalid)
            - bool: True if number_b was successfully normalized

    Logic:
        1. Normalizes number_a first
        2. If number_a is valid and 10-11 digits, extracts area code (first 2 digits)
        3. Uses extracted area code as context for normalizing number_b
        4. Returns normalization results for both numbers

    Examples:
        >>> normalize_number_pair("11999999999", "88888888")
        ('11999999999', True, '1188888888', True)
        >>> normalize_number_pair("invalid", "11999999999")
        ('invalid', False, '11999999999', True)
        >>> normalize_number_pair("1133334444", "22225555")
        ('1133334444', True, '1122225555', True)

    Use Case:
        Particularly useful for Call Detail Records (CDRs) where the originating
        number can provide geographic context for local destination numbers.
    """
    normalized_number_a, is_number_a_valid = normalize_number(number_a)

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


@pandas_udf(SPARK_NORMALIZE_NUMBER_RETURN_SCHEMA)
def spark_normalize_number(number_a_series: pd.Series) -> pd.DataFrame:
    # Processar em batch (vetorizado)
    results = []
    for number_a in number_a_series:
        results.append(normalize_number(number_a))

    return pd.DataFrame(
        results,
        columns=[
            "numero_formatado",
            "numero_valido",
        ],
    )
