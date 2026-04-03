from datetime import date

from src.parsers.common import parse_number, safe_parse_date


def test_parse_number_handles_russian_decimal() -> None:
    assert parse_number("12,5") == 12.5
    assert parse_number("1 234,56") == 1234.56
    assert parse_number("9.1") == 9.1
    assert parse_number("*") is None


def test_safe_parse_date_handles_text_and_excel_serial() -> None:
    assert safe_parse_date("05.03.2011") == date(2011, 3, 5)
    assert safe_parse_date(40607) == date(2011, 3, 5)
    assert safe_parse_date("") is None
