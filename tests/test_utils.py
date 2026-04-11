from datetime import date

import pandas as pd
import pytest

from src.parsers.common import merge_with_checks, parse_number, safe_parse_date


def test_parse_number_handles_russian_decimal() -> None:
    assert parse_number("12,5") == 12.5
    assert parse_number("1 234,56") == 1234.56
    assert parse_number("9.1") == 9.1
    assert parse_number("*") is None


def test_safe_parse_date_handles_text_and_excel_serial() -> None:
    assert safe_parse_date("05.03.2011") == date(2011, 3, 5)
    assert safe_parse_date(40607) == date(2011, 3, 5)
    assert safe_parse_date("") is None


def test_merge_with_checks_accepts_many_to_one_join() -> None:
    left = pd.DataFrame({"site_id": ["a", "a", "b"], "value": [1, 2, 3]})
    right = pd.DataFrame({"site_id": ["a", "b"], "site_name": ["A", "B"]})

    result = merge_with_checks(
        left,
        right,
        on="site_id",
        how="left",
        validate="many_to_one",
        relationship_name="test merge",
    )

    assert list(result["site_name"]) == ["A", "A", "B"]


def test_merge_with_checks_raises_on_unmatched_left_rows() -> None:
    left = pd.DataFrame({"site_id": ["a", "missing"]})
    right = pd.DataFrame({"site_id": ["a"], "site_name": ["A"]})

    with pytest.raises(ValueError, match="test merge"):
        merge_with_checks(
            left,
            right,
            on="site_id",
            how="left",
            validate="many_to_one",
            relationship_name="test merge",
        )
