import itertools

import pytest

from neptune_fetcher.util import (
    batched_paths,
    escape_nql_criterion,
)


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


@pytest.mark.parametrize(
    "paths, batch_size, query_limit, expected",
    (
        ([], 10, 20, []),
        (["1", "2", "3"], 1, 1, [["1"], ["2"], ["3"]]),
        (["12", "34", "56"], 2, 100, [["12", "34"], ["56"]]),
        (["12", "34", "56"], 2, 6, [["12", "34"], ["56"]]),
        (["12", "34", "56789"], 100, 4, [["12", "34"], ["56789"]]),
        (["12", "345678"], 100, 2, [["12"], ["345678"]]),
        (["1234", "5678"], 100, 2, [["1234"], ["5678"]]),
        # Unicode characters encode to more than 1 byte
        (["ą", "β", "γ"], 100, 1, [["ą"], ["β"], ["γ"]]),
        (["", "", ""], 2, 100, [["", ""], [""]]),
    ),
)
def test_batched_paths(paths, batch_size, query_limit, expected):
    result = batched_paths(paths, batch_size, query_limit)
    assert list(itertools.chain(*result)) == paths
    assert result == expected
