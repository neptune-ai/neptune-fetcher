# tests for src/neptune_fetcher/internal/util.py

import pytest

from neptune_fetcher.internal import util


def test__validate_string_or_string_list():
    util._validate_string_or_string_list(None, "foo")
    util._validate_string_or_string_list("abc", "foo")
    util._validate_string_or_string_list(["a", "b"], "foo")
    with pytest.raises(ValueError):
        util._validate_string_or_string_list(123, "foo")
    with pytest.raises(ValueError):
        util._validate_string_or_string_list([1, 2], "foo")


def test__validate_string_list():
    util._validate_string_list(None, "bar")
    util._validate_string_list(["a", "b"], "bar")
    with pytest.raises(ValueError):
        util._validate_string_list("abc", "bar")
    with pytest.raises(ValueError):
        util._validate_string_list([1, 2], "bar")
    with pytest.raises(ValueError):
        util._validate_string_list([], "bar", disallow_empty=True)


def test__validate_list_of_allowed_values():
    util._validate_list_of_allowed_values(["a", "b"], {"a", "b", "c"}, "baz")
    with pytest.raises(ValueError):
        util._validate_list_of_allowed_values(["a", "x"], {"a", "b"}, "baz")
    with pytest.raises(ValueError):
        util._validate_list_of_allowed_values("a", {"a"}, "baz")


def test__validate_allowed_value():
    util._validate_allowed_value(None, {"a", "b"}, "qux")
    util._validate_allowed_value("a", {"a", "b"}, "qux")
    with pytest.raises(ValueError):
        util._validate_allowed_value("x", {"a", "b"}, "qux")
    with pytest.raises(ValueError):
        util._validate_allowed_value(123, {"a", "b"}, "qux")


def test__is_string_sequence():
    assert util._is_string_sequence(["a", "b"])
    assert util._is_string_sequence(("x", "y"))
    assert not util._is_string_sequence("abc")
    assert not util._is_string_sequence([1, 2])
    assert not util._is_string_sequence(None)
    assert not util._is_string_sequence(123)
    assert util._is_string_sequence([])
    assert util._is_string_sequence(())
