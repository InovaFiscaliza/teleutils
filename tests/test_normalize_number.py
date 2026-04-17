"""
Tests for the normalize_number function in teleutils.preprocessing.number_format.
"""

import pytest

from teleutils.preprocessing.number_format import normalize_number

# ---------------------------------------------------------------------------
# Valid numbers – should return (normalized_str, True)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        # SMP – mobile with formatting
        ("(11) 99999-9999", "11999999999"),
        ("11 9 9999-9999", "11999999999"),
        ("11999999999", "11999999999"),
        # SMP – other area codes
        ("21987654321", "21987654321"),
        ("51991234567", "51991234567"),
        # STFC – fixed-line
        ("(11) 3333-4444", "1133334444"),
        ("1133334444", "1133334444"),
        ("2122223333", "2122223333"),
        # CNG – toll-free 0800 (leading '0' is stripped as national prefix)
        ("0800-123-4567", "8001234567"),
        ("08001234567", "8001234567"),
        # CNG – 300x
        ("3001234567", "3001234567"),
        # Numbers with country code 55
        ("5511999999999", "11999999999"),
        ("551133334444", "1133334444"),
        # National prefix stripped
        ("01133334444", "1133334444"),
        ("011999999999", "11999999999"),
        # Collect call prefix 90 stripped
        ("9011999999999", "11999999999"),
        # Collect call prefix 9090 stripped
        ("909011999999999", "11999999999"),
        # Integer input
        (11999999999, "11999999999"),
        # Semicolon-separated – takes first number
        ("11999999999;21888888888", "11999999999"),
        # Filler character 'f'
        ("f11999999999", "11999999999"),
        ("11f999999999", "11999999999"),
    ],
)
def test_normalize_number_valid(raw, expected):
    result, is_valid = normalize_number(raw)
    assert is_valid is True
    assert result == expected


# ---------------------------------------------------------------------------
# Valid short numbers with national_destination_code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, ndc, expected",
    [
        # 9-digit mobile
        ("999999999", "11", "11999999999"),
        # 8-digit fixed-line
        ("33334444", "11", "1133334444"),
        ("22223333", "21", "2122223333"),
        # area code already present – ndc must not be added again
        ("11999999999", "21", "11999999999"),
    ],
)
def test_normalize_number_with_ndc(raw, ndc, expected):
    result, is_valid = normalize_number(raw, national_destination_code=ndc)
    assert is_valid is True
    assert result == expected


# ---------------------------------------------------------------------------
# Small / utility service numbers (SUP)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "190",  # Polícia Militar
        "192",  # SAMU
        "193",  # Bombeiros
        "199",  # Defesa Civil
    ],
)
def test_normalize_number_sup(raw):
    result, is_valid = normalize_number(raw)
    assert is_valid is True
    assert result == raw


# ---------------------------------------------------------------------------
# Invalid numbers – should return (original_str, False)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "invalid",
        "00000000000",  # all zeros
        "12345",  # too short, no valid pattern
        "9" * 14,  # too long (> 13 digits after cleaning)
        "",
    ],
)
def test_normalize_number_invalid(raw):
    result, is_valid = normalize_number(raw)
    assert is_valid is False


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


def test_normalize_number_returns_tuple():
    result = normalize_number("11999999999")
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], str)
    assert isinstance(result[1], bool)


def test_normalize_number_invalid_preserves_original():
    """Original string must be returned unchanged when normalization fails."""
    raw = "numero_invalido"
    result, is_valid = normalize_number(raw)
    assert is_valid is False
    assert result == raw
