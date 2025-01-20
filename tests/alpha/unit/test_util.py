import pytest

from neptune_fetcher.alpha.util import escape_nql_criterion


@pytest.mark.parametrize(
    "regex, expect",
    (
        ("\"'", r"\"'"),  # Escaped double quotes
        ("abc\\", "abc\\\\"),  # Backslash -> double backslash
        ("a\\d'b\"c\\", "a\\\\d'b\\\"c\\\\"),
    ),
)
def test_escape_nql_criterion(regex, expect):
    assert escape_nql_criterion(regex) == expect
