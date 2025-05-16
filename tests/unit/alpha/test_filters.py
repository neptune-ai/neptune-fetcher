from datetime import datetime

import pytest

from neptune_fetcher.internal.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.internal.retrieval import attribute_types as types


def test_attribute_valid_values():
    # Test valid cases - should not raise exceptions
    Attribute(name="test")  # minimal case
    Attribute(name="test", aggregation="last")
    Attribute(name="test", type="float")
    Attribute(name="test", aggregation="variance", type="float_series")


def test_attribute_invalid_aggregation():
    # Test invalid aggregation values
    with pytest.raises(ValueError) as exc_info:
        Attribute(name="test", aggregation="invalid_agg")
    assert f"aggregation must be one of: {sorted(types.ALL_AGGREGATIONS)}" in str(exc_info.value)


def test_attribute_invalid_type():
    # Test invalid type values
    with pytest.raises(ValueError) as exc_info:
        Attribute(name="test", type="invalid_type")
    assert f"type must be one of: {sorted(types.ALL_TYPES)}" in str(exc_info.value)


@pytest.mark.parametrize("aggregation", types.ALL_AGGREGATIONS)
def test_attribute_all_valid_aggregations(aggregation):
    # Test all valid aggregation values
    attr = Attribute(name="test", aggregation=aggregation)
    assert attr.aggregation == aggregation


@pytest.mark.parametrize("type_", types.ALL_TYPES)
def test_attribute_all_valid_types(type_):
    # Test all valid type values
    attr = Attribute(name="test", type=type_)
    assert attr.type == type_


def test_filter_valid_values():
    # Test valid cases - should not raise exceptions
    Filter.eq("test", "value")  # str value
    Filter.eq("test", 42)  # int value
    Filter.eq("test", 3.14)  # float value
    Filter.eq("test", datetime.now())  # datetime value

    # Test all operators with string value
    Filter.ne("test", "value")
    Filter.gt("test", "value")
    Filter.ge("test", "value")
    Filter.lt("test", "value")
    Filter.le("test", "value")
    Filter.matches_all("test", "value")
    Filter.matches_none("test", "value")
    Filter.contains_all("test", "value")
    Filter.contains_none("test", "value")


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
            Filter.eq("test", invalid_value)  # type: ignore
        assert "Invalid value type:" in str(exc_info.value)
        assert "Expected int, float, str, or datetime" in str(exc_info.value)


@pytest.mark.parametrize(
    "method,operator",
    [
        (Filter.eq, "=="),
        (Filter.ne, "!="),
        (Filter.gt, ">"),
        (Filter.ge, ">="),
        (Filter.lt, "<"),
        (Filter.le, "<="),
        (Filter.matches_all, "MATCHES"),
        (Filter.matches_none, "NOT MATCHES"),
        (Filter.contains_all, "CONTAINS"),
        (Filter.contains_none, "NOT CONTAINS"),
    ],
)
def test_filter_operators(method, operator):
    # Test that each filter method creates predicate with correct operator
    attr = "test"
    value = "value"
    filter_obj = method(attr, value)
    assert filter_obj.operator == operator
    assert isinstance(filter_obj.attribute, Attribute)
    assert filter_obj.value == value


def test_filter_with_attribute_object():
    # Test using Attribute object instead of string
    attr = Attribute(name="test", type="string")
    filter_obj = Filter.eq(attr, "value")
    assert filter_obj.attribute == attr
    assert filter_obj.value == "value"


def test_filter_query_string_escaping():
    # Test that special characters in values are properly escaped
    filter_obj = Filter.eq("test", 'value with "quotes" and \\backslashes\\')
    query = filter_obj.to_query()
    assert '"value with \\"quotes\\" and \\\\backslashes\\\\"' in query


def test_filter_datetime_formatting():
    # Test datetime value formatting in query
    now = datetime.now()
    filter_obj = Filter.eq("test", now)
    query = filter_obj.to_query()
    assert now.astimezone().isoformat() in query


def test_attribute_filter_valid_values():
    # Test valid cases
    AttributeFilter()  # default values
    AttributeFilter(name_eq="test")  # string
    AttributeFilter(name_eq=["test1", "test2"])  # list of strings
    AttributeFilter(type_in=["float", "int"])  # valid types
    AttributeFilter(name_matches_all="test")  # string
    AttributeFilter(name_matches_all=["test1", "test2"])  # list of strings
    AttributeFilter(name_matches_none="test")  # string
    AttributeFilter(name_matches_none=["test1", "test2"])  # list of strings
    AttributeFilter(aggregations=["last", "min"])  # valid aggregations


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
            AttributeFilter(name_eq=invalid_value)  # type: ignore
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
            AttributeFilter(type_in=invalid_value)  # type: ignore
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
            AttributeFilter(name_matches_all=invalid_value)  # type: ignore
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
            AttributeFilter(name_matches_none=invalid_value)  # type: ignore
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
            AttributeFilter(aggregations=invalid_value)  # type: ignore
        assert f"aggregations must be a list of valid values: {sorted(types.ALL_AGGREGATIONS)}" in str(exc_info.value)


@pytest.mark.parametrize("valid_type", sorted(types.ALL_TYPES))
def test_all_valid_types(valid_type):
    # Test each valid type individually
    attr_filter = AttributeFilter(type_in=[valid_type])
    assert valid_type in attr_filter.type_in


@pytest.mark.parametrize("valid_agg", sorted(types.ALL_AGGREGATIONS))
def test_all_valid_aggregations(valid_agg):
    # Test each valid aggregation individually
    attr_filter = AttributeFilter(aggregations=[valid_agg])
    assert valid_agg in attr_filter.aggregations
