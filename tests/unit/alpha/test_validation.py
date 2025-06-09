import pytest

from neptune_fetcher.internal.composition.validation import (
    validate_attribute_filter_type,
    validate_include_time,
    validate_limit,
    validate_sort_direction,
    validate_step_range,
    validate_tail_limit,
)
from neptune_fetcher.internal.filters import (
    _AttributeFilter,
    _AttributeFilterAlternative,
)


def test_validate_limit():
    # Valid cases
    validate_limit(None)

    validate_limit(1)
    validate_limit(100)

    # Invalid cases
    with pytest.raises(ValueError, match="must be None or an integer"):
        validate_limit(1.5)

    with pytest.raises(ValueError, match="must be None or an integer"):
        validate_limit("1")

    with pytest.raises(ValueError, match="must be greater than 0"):
        validate_limit(0)

    with pytest.raises(ValueError, match="must be greater than 0"):
        validate_limit(-1)


def test_validate_sort_direction():
    # Valid cases
    validate_sort_direction("asc")
    validate_sort_direction("desc")

    # Invalid cases
    with pytest.raises(ValueError, match="sort_direction 'invalid' is invalid; must be 'asc' or 'desc'"):
        validate_sort_direction("invalid")


def test_validate_step_range():
    # Valid cases
    validate_step_range((None, None))
    validate_step_range((0, None))
    validate_step_range((None, 10))
    validate_step_range((0, 10))
    validate_step_range((0.5, 10.5))
    validate_step_range((0, 0))  # equal values are allowed


def test_validate_step_range_invalid():
    # Invalid types
    with pytest.raises(ValueError, match="must be a tuple of two values"):
        validate_step_range([None, None])

    with pytest.raises(ValueError, match="must be a tuple of two values"):
        validate_step_range((None,))

    # Invalid value types
    with pytest.raises(ValueError, match="start must be None or a number"):
        validate_step_range(("0", None))

    with pytest.raises(ValueError, match="end must be None or a number"):
        validate_step_range((None, "10"))

    # Invalid range
    with pytest.raises(ValueError, match="start must be less than or equal to end"):
        validate_step_range((10, 0))


def test_validate_tail_limit():
    # Valid cases
    validate_tail_limit(None)
    validate_tail_limit(1)
    validate_tail_limit(100)

    # Invalid cases
    with pytest.raises(ValueError, match="must be None or an integer"):
        validate_tail_limit(1.5)

    with pytest.raises(ValueError, match="must be None or an integer"):
        validate_tail_limit("1")

    with pytest.raises(ValueError, match="must be greater than 0"):
        validate_tail_limit(0)

    with pytest.raises(ValueError, match="must be greater than 0"):
        validate_tail_limit(-1)


def test_validate_include_time():
    # Valid cases
    validate_include_time(None)
    validate_include_time("absolute")


def test_validate_include_time_invalid():
    # Invalid cases
    with pytest.raises(ValueError, match="include_time must be 'absolute'"):
        validate_include_time("invalid")

    with pytest.raises(ValueError, match="include_time must be 'absolute'"):
        validate_include_time("relative")


@pytest.mark.parametrize(
    "attribute_filter, type_in",
    [
        (_AttributeFilter("a"), "float_series"),
        (_AttributeFilter("a", type_in=["string_series"]), "string_series"),
        (_AttributeFilter("a", type_in=["float_series"]), "float_series"),
        (_AttributeFilter("a", type_in=["string_series", "float_series"]), "float_series"),
        (_AttributeFilter("a", type_in=["string_series", "float_series"]), "string_series"),
        (
            _AttributeFilter("a", type_in=["string_series"]) | _AttributeFilter("b", type_in=["string_series"]),
            "string_series",
        ),
        (
            _AttributeFilter("a", type_in=["string_series", "float_series"])
            | _AttributeFilter("b", type_in=["float_series"]),
            "float_series",
        ),
    ],
)
def test_validate_attribute_filter_type_valid(attribute_filter, type_in):
    # Valid cases
    validate_attribute_filter_type(attribute_filter, type_in=type_in)
    if isinstance(attribute_filter, _AttributeFilter):
        assert attribute_filter.type_in == [type_in]
    elif isinstance(attribute_filter, _AttributeFilterAlternative):
        for child in attribute_filter.filters:
            assert isinstance(child, _AttributeFilter)


@pytest.mark.parametrize(
    "attribute_filter, type_in",
    [
        (_AttributeFilter("a", type_in=[]), "float_series"),
        (_AttributeFilter("a", type_in=["string_series"]), "float_series"),
        (_AttributeFilter("a", type_in=["float_series"]), "string_series"),
        (_AttributeFilter("a", type_in=["string_series", "float_series"]), "int"),
        (_AttributeFilter("a", type_in=["float_series"]) | _AttributeFilter(type_in=["string_series"]), "int"),
        (_AttributeFilter("a", type_in=["int"]) | _AttributeFilter(type_in=["string_series"]), "int"),
        (_AttributeFilter("a", type_in=["float_series"]) | _AttributeFilter(type_in=["int"]), "int"),
    ],
)
def test_validate_attribute_filter_type_invalid(attribute_filter, type_in):
    # Valid cases
    with pytest.raises(ValueError, match=f"Only {type_in} type is supported for attribute filters in this function"):
        validate_attribute_filter_type(attribute_filter, type_in=type_in)
