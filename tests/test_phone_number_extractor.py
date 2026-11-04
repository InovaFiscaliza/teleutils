"""Tests for the current phone number extraction contract."""

import pytest

from teleutils._config import SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE
from teleutils.preprocessing import extract_number as public_extract_number
from teleutils.preprocessing.phone_number_extractor import (
    ExtractionResult,
    extract_number,
)


@pytest.mark.parametrize(
    "subscriber_number, expected_number",
    [
        ("11987654321", "11987654321"),
        ("21987654321", "21987654321"),
        ("51991234567", "51991234567"),
        ("1134567890", "1134567890"),
        ("1176543210", "1176543210"),
        ("08001234567", "8001234567"),
        ("800123456", "800123456"),
        ("03001234567", "3001234567"),
        ("03031234567", "3031234567"),
        ("5511987654321", "11987654321"),
        ("01134567890", "1134567890"),
        ("9011987654321", "11987654321"),
        ("9090987654321", "987654321"),
        (11987654321, "11987654321"),
        ("11987654321F", "11987654321"),
        ("1198765432100000000", "11987654321"),
        ("11987654321;21991234567", "21991234567"),
        ("11987654321;2134567890", "11987654321"),
        ("prefix11987654321suffix", "11987654321"),
    ],
)
def test_extract_number(subscriber_number, expected_number):
    result = extract_number(subscriber_number=subscriber_number)
    assert result == ExtractionResult(expected_number, True)


@pytest.mark.parametrize("subscriber_number", ["987654321", "34567890", "76543210"])
def test_extract_number_keeps_local_number_without_adding_area_code(subscriber_number):
    assert extract_number(subscriber_number) == ExtractionResult(
        subscriber_number, True
    )


@pytest.mark.parametrize("subscriber_number", ["190", "192", "193", "1051"])
def test_extract_number_utility_services(subscriber_number):
    assert extract_number(subscriber_number) == ExtractionResult(
        subscriber_number, True
    )


def test_extract_number_utility_service_removes_area_code():
    assert extract_number("11190") == ExtractionResult("190", True)


@pytest.mark.parametrize(
    "subscriber_number",
    [
        None,
        "",
        0,
        False,
        "invalid",
        "00000000000",
        "12345",
        "9" * 14,
        "11999999999",
        "1133334444",
        "199",
        "1331",
        "13312",
        "(11) 98765-4321",
        "11 98765-4321",
        "0800-123-4567",
    ],
)
def test_extract_number_failure_returns_sentinel(subscriber_number):
    result = extract_number(subscriber_number=subscriber_number)
    assert result == ExtractionResult(SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE, False)


def test_extract_number_return_type_and_public_export():
    assert public_extract_number is extract_number
    result = extract_number(subscriber_number="11987654321")
    assert isinstance(result, ExtractionResult)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result.number, str)
    assert isinstance(result.success, bool)
