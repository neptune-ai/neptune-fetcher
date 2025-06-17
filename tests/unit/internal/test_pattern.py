import pytest

from neptune_fetcher.internal.filters import (
    _Attribute,
    _AttributeFilter,
    _AttributeNameFilter,
)
from neptune_fetcher.internal.pattern import (
    Alternative,
    Conjunction,
    build_extended_regex_attribute_filter,
    build_extended_regex_filter,
    parse_extended_regex,
)


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ("", Alternative([Conjunction([""], [])])),
        (" ", Alternative([Conjunction([""], [])])),
        ("a", Alternative([Conjunction(["a"], [])])),
        ("a b", Alternative([Conjunction(["a b"], [])])),  # TODO: I don't see a reason to disallow this tbh
        (r"\x21a\x20b", Alternative([Conjunction(["!a b"], [])])),
        (" a ", Alternative([Conjunction(["a"], [])])),
        ("a & b", Alternative([Conjunction(["a", "b"], [])])),
        (" \r a \t\t  & \n b  ", Alternative([Conjunction(["a", "b"], [])])),
        ("a | b", Alternative([Conjunction(["a"], []), Conjunction(["b"], [])])),
        ("!a", Alternative([Conjunction([], ["a"])])),
        ("! a", Alternative([Conjunction([], ["a"])])),
        (" ! a", Alternative([Conjunction([], ["a"])])),
        ("a & b | c & d", Alternative([Conjunction(["a", "b"], []), Conjunction(["c", "d"], [])])),
        ("a | b & c | d", Alternative([Conjunction(["a"], []), Conjunction(["b", "c"], []), Conjunction(["d"], [])])),
        ("a & b & !c", Alternative([Conjunction(["a", "b"], ["c"])])),
        ("a & ! b & !c", Alternative([Conjunction(["a"], ["b", "c"])])),
        (" ! a & ! b & !c", Alternative([Conjunction([], ["a", "b", "c"])])),
        ("( a & b )", Alternative([Conjunction(["( a", "b )"], [])])),
        ("& a & b|", Alternative([Conjunction(["& a", "b|"], [])])),
        ("&", Alternative([Conjunction(["&"], [])])),
        (" & ", Alternative([Conjunction(["&"], [])])),
        ("& & &", Alternative([Conjunction(["&", "&"], [])])),
        ("& & & &", Alternative([Conjunction(["&", "& &"], [])])),
        ("|", Alternative([Conjunction(["|"], [])])),
        (" | ", Alternative([Conjunction(["|"], [])])),
        ("| | |", Alternative([Conjunction(["|"], []), Conjunction(["|"], [])])),
        ("| | | |", Alternative([Conjunction(["|"], []), Conjunction(["| |"], [])])),
        ("!", Alternative([Conjunction([], [""])])),
        ("!!", Alternative([Conjunction([], ["!"])])),
        ("!! !!", Alternative([Conjunction([], ["! !!"])])),
        ("[a-f]+[ghi]*[x-z]{3,5}", Alternative([Conjunction(["[a-f]+[ghi]*[x-z]{3,5}"], [])])),
        ("![a-f]+[ghi]*[x-z]{3,5}", Alternative([Conjunction([], ["[a-f]+[ghi]*[x-z]{3,5}"])])),
        ("! [a-f]+[ghi]*[x-z]{3,5}", Alternative([Conjunction([], ["[a-f]+[ghi]*[x-z]{3,5}"])])),
    ],
)
def test_parse_extended_regex(pattern, expected):
    result = parse_extended_regex(pattern)

    assert result == expected


@pytest.mark.parametrize(
    "attribute,pattern,query",
    [
        (_Attribute("x"), "", '((`x` MATCHES ""))'),
        (_Attribute("x"), "a", '((`x` MATCHES "a"))'),
        (_Attribute("x", type="int"), "a", '((`x`:int MATCHES "a"))'),
        (_Attribute("x"), " ! a", '((`x` NOT MATCHES "a"))'),
        (_Attribute("x"), "a & b", '((`x` MATCHES "a") AND (`x` MATCHES "b"))'),
        (_Attribute("x"), "a | b", '((`x` MATCHES "a")) OR ((`x` MATCHES "b"))'),
        (
            _Attribute("x"),
            "a & b | c & d",
            '((`x` MATCHES "a") AND (`x` MATCHES "b")) OR ((`x` MATCHES "c") AND (`x` MATCHES "d"))',
        ),
        (
            _Attribute("x"),
            "a | b & c | d",
            '((`x` MATCHES "a")) OR ((`x` MATCHES "b") AND (`x` MATCHES "c")) OR ((`x` MATCHES "d"))',
        ),
        (
            _Attribute("x"),
            "! a | b & ! c | !d",
            '((`x` NOT MATCHES "a")) OR ((`x` MATCHES "b") AND (`x` NOT MATCHES "c")) OR ((`x` NOT MATCHES "d"))',
        ),
        (_Attribute("x"), "& & & &", '((`x` MATCHES "&") AND (`x` MATCHES "& &"))'),
    ],
)
def test_build_extended_regex_filter(attribute, pattern, query):
    result = build_extended_regex_filter(attribute, pattern)

    assert result.to_query() == query


@pytest.mark.parametrize(
    "type_in,pattern,expected",
    [
        (
            ["string"],
            "",
            _AttributeFilter(type_in=["string"], must_match_any=[_AttributeNameFilter(must_match_regexes=[""])]),
        ),
        (
            ["string"],
            "a",
            _AttributeFilter(type_in=["string"], must_match_any=[_AttributeNameFilter(must_match_regexes=["a"])]),
        ),
        (
            ["int"],
            "a",
            _AttributeFilter(type_in=["int"], must_match_any=[_AttributeNameFilter(must_match_regexes=["a"])]),
        ),
        (
            ["string"],
            "!a",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[_AttributeNameFilter(must_not_match_regexes=["a"])],
            ),
        ),
        (
            ["string"],
            " ! a",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[_AttributeNameFilter(must_not_match_regexes=["a"])],
            ),
        ),
        (
            ["string"],
            "a & b",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[_AttributeNameFilter(must_match_regexes=["a", "b"])],
            ),
        ),
        (
            ["string"],
            "a | b",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[
                    _AttributeNameFilter(must_match_regexes=["a"]),
                    _AttributeNameFilter(must_match_regexes=["b"]),
                ],
            ),
        ),
        (
            ["string"],
            "a & b | c & d",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[
                    _AttributeNameFilter(must_match_regexes=["a", "b"]),
                    _AttributeNameFilter(must_match_regexes=["c", "d"]),
                ],
            ),
        ),
        (
            ["string"],
            "a | b & c | d",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[
                    _AttributeNameFilter(must_match_regexes=["a"]),
                    _AttributeNameFilter(must_match_regexes=["b", "c"]),
                    _AttributeNameFilter(must_match_regexes=["d"]),
                ],
            ),
        ),
        (
            ["string"],
            "! a | b & ! c | !d",
            _AttributeFilter(
                type_in=["string"],
                must_match_any=[
                    _AttributeNameFilter(must_not_match_regexes=["a"]),
                    _AttributeNameFilter(must_match_regexes=["b"], must_not_match_regexes=["c"]),
                    _AttributeNameFilter(must_not_match_regexes=["d"]),
                ],
            ),
        ),
    ],
)
def test_build_extended_regex_attribute_filter(type_in, pattern, expected):
    result = build_extended_regex_attribute_filter(pattern, type_in)

    assert result == expected
