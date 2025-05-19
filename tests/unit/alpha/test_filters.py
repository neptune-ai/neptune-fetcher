from datetime import datetime

import pytest

from neptune_fetcher.internal.filters import (
    AttributeInternal,
    AttributeFilterInternal,
    FilterInternal,
)
from neptune_fetcher.internal.retrieval import attribute_types as types


def test_attribute_valid_values():
    # Test valid cases - should not raise exceptions
    AttributeInternal(name="test")  # minimal case
    AttributeInternal(name="test", aggregation="last")
    AttributeInternal(name="test", type="float")
    AttributeInternal(name="test", aggregation="variance", type="float_series")


def test_attribute_invalid_aggregation():
    # Test invalid aggregation values
    with pytest.raises(ValueError) as exc_info:
        AttributeInternal(name="test", aggregation="invalid_agg")
    assert f"aggregation must be one of: {sorted(types.ALL_AGGREGATIONS)}" in str(exc_info.value)


def test_attribute_invalid_type():
    # Test invalid type values
    with pytest.raises(ValueError) as exc_info:
        AttributeInternal(name="test", type="invalid_type")
    assert f"type must be one of: {sorted(types.ALL_TYPES)}" in str(exc_info.value)


@pytest.mark.parametrize("aggregation", types.ALL_AGGREGATIONS)
def test_attribute_all_valid_aggregations(aggregation):
    # Test all valid aggregation values
    attr = AttributeInternal(name="test", aggregation=aggregation)
    assert attr.aggregation == aggregation


@pytest.mark.parametrize("type_", types.ALL_TYPES)
def test_attribute_all_valid_types(type_):
    # Test all valid type values
    attr = AttributeInternal(name="test", type=type_)
    assert attr.type == type_


def test_filter_valid_values():
    # Test valid cases - should not raise exceptions
    FilterInternal.eq("test", "value")  # str value
    FilterInternal.eq("test", 42)  # int value
    FilterInternal.eq("test", 3.14)  # float value
    FilterInternal.eq("test", datetime.now())  # datetime value

    # Test all operators with string value
    FilterInternal.ne("test", "value")
    FilterInternal.gt("test", "value")
    FilterInternal.ge("test", "value")
    FilterInternal.lt("test", "value")
    FilterInternal.le("test", "value")
    FilterInternal.matches_all("test", "value")
    FilterInternal.matches_none("test", "value")
    FilterInternal.contains_all("test", "value")
    FilterInternal.contains_none("test", "value")


def test_filter_invalid_value_type():
    # Test invalid value types
    invalid_values = [
        [],  # list
        {},  # dict
        None,  # None
        (1, 2),  # tuple
    ]

    for invalid_value in invalid_values:
        with pytest.raises(TypeError) as exc_info:
            FilterInternal.eq("test", invalid_value)  # type: ignore
        assert "Invalid value type:" in str(exc_info.value)
        assert "Expected int, float, str, or datetime" in str(exc_info.value)


@pytest.mark.parametrize(
    "method,operator",
    [
        (FilterInternal.eq, "=="),
        (FilterInternal.ne, "!="),
        (FilterInternal.gt, ">"),
        (FilterInternal.ge, ">="),
        (FilterInternal.lt, "<"),
        (FilterInternal.le, "<="),
        (FilterInternal.matches_all, "MATCHES"),
        (FilterInternal.matches_none, "NOT MATCHES"),
        (FilterInternal.contains_all, "CONTAINS"),
        (FilterInternal.contains_none, "NOT CONTAINS"),
    ],
)
def test_filter_operators(method, operator):
    # Test that each filter method creates predicate with correct operator
    attr = "test"
    value = "value"
    filter_obj = method(attr, value)
    assert filter_obj.operator == operator
    assert isinstance(filter_obj.attribute, AttributeInternal)
    assert filter_obj.value == value


def test_filter_with_attribute_object():
    # Test using Attribute object instead of string
    attr = AttributeInternal(name="test", type="string")
    filter_obj = FilterInternal.eq(attr, "value")
    assert filter_obj.attribute == attr
    assert filter_obj.value == "value"


def test_filter_query_string_escaping():
    # Test that special characters in values are properly escaped
    filter_obj = FilterInternal.eq("test", 'value with "quotes" and \\backslashes\\')
    query = filter_obj.to_query()
    assert '"value with \\"quotes\\" and \\\\backslashes\\\\"' in query


def test_filter_datetime_formatting():
    # Test datetime value formatting in query
    now = datetime.now()
    filter_obj = FilterInternal.eq("test", now)
    query = filter_obj.to_query()
    assert now.astimezone().isoformat() in query


def test_attribute_filter_valid_values():
    # Test valid cases
    AttributeFilterInternal()  # default values
    AttributeFilterInternal(name_eq="test")  # string
    AttributeFilterInternal(name_eq=["test1", "test2"])  # list of strings
    AttributeFilterInternal(type_in=["float", "int"])  # valid types
    AttributeFilterInternal(name_matches_all="test")  # string
    AttributeFilterInternal(name_matches_all=["test1", "test2"])  # list of strings
    AttributeFilterInternal(name_matches_none="test")  # string
    AttributeFilterInternal(name_matches_none=["test1", "test2"])  # list of strings
    AttributeFilterInternal(aggregations=["last", "min"])  # valid aggregations


def test_name_eq_validation():
    # Test invalid name_eq values
    invalid_values = [
        42,  # int
        3.14,  # float
        True,  # bool
        ["test", 42],  # list with non-string
        [1, 2],  # list of numbers
    ]

    for invalid_value in invalid_values:
        with pytest.raises(ValueError) as exc_info:
            AttributeFilterInternal(name_eq=invalid_value)  # type: ignore
        assert "name_eq must be a string or list of strings" in str(exc_info.value)


def test_type_in_validation():
    # Test invalid type_in values
    invalid_values = [
        "invalid_type",  # string instead of list
        ["invalid_type"],  # list with invalid type
        ["float", "invalid_type"],  # list with mix of valid and invalid
        [42, "float"],  # list with non-string
    ]

    for invalid_value in invalid_values:
        with pytest.raises(ValueError) as exc_info:
            AttributeFilterInternal(type_in=invalid_value)  # type: ignore
        assert f"type_in must be a list of valid values: {sorted(types.ALL_TYPES)}" in str(exc_info.value)


def test_name_matches_all_validation():
    # Test invalid name_matches_all values
    invalid_values = [
        42,  # int
        3.14,  # float
        True,  # bool
        ["test", 42],  # list with non-string
        [1, 2],  # list of numbers
    ]

    for invalid_value in invalid_values:
        with pytest.raises(ValueError) as exc_info:
            AttributeFilterInternal(name_matches_all=invalid_value)  # type: ignore
        assert "name_matches_all must be a string or list of strings" in str(exc_info.value)


def test_name_matches_none_validation():
    # Test invalid name_matches_none values
    invalid_values = [
        42,  # int
        3.14,  # float
        True,  # bool
        ["test", 42],  # list with non-string
        [1, 2],  # list of numbers
    ]

    for invalid_value in invalid_values:
        with pytest.raises(ValueError) as exc_info:
            AttributeFilterInternal(name_matches_none=invalid_value)  # type: ignore
        assert "name_matches_none must be a string or list of strings" in str(exc_info.value)


def test_aggregations_validation():
    # Test invalid aggregations values
    invalid_values = [
        "last",  # string instead of list
        ["invalid_agg"],  # list with invalid aggregation
        ["last", "invalid_agg"],  # list with mix of valid and invalid
        [42, "last"],  # list with non-string
    ]

    for invalid_value in invalid_values:
        with pytest.raises(ValueError) as exc_info:
            AttributeFilterInternal(aggregations=invalid_value)  # type: ignore
        assert f"aggregations must be a list of valid values: {sorted(types.ALL_AGGREGATIONS)}" in str(exc_info.value)


@pytest.mark.parametrize("valid_type", sorted(types.ALL_TYPES))
def test_all_valid_types(valid_type):
    # Test each valid type individually
    attr_filter = AttributeFilterInternal(type_in=[valid_type])
    assert valid_type in attr_filter.type_in


@pytest.mark.parametrize("valid_agg", sorted(types.ALL_AGGREGATIONS))
def test_all_valid_aggregations(valid_agg):
    # Test each valid aggregation individually
    attr_filter = AttributeFilterInternal(aggregations=[valid_agg])
    assert valid_agg in attr_filter.aggregations
