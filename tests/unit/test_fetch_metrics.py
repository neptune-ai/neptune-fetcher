import pytest

from neptune_fetcher.internal.composition.fetch_metrics import (
    _validate_include_time,
    _validate_step_range,
    _validate_tail_limit,
)


def test_validate_step_range():
    # Valid cases
    _validate_step_range((None, None))
    _validate_step_range((0, None))
    _validate_step_range((None, 10))
    _validate_step_range((0, 10))
    _validate_step_range((0.5, 10.5))
    _validate_step_range((0, 0))  # equal values are allowed


def test_validate_step_range_invalid():
    # Invalid types
    with pytest.raises(ValueError, match="must be a tuple of two values"):
        _validate_step_range([None, None])

    with pytest.raises(ValueError, match="must be a tuple of two values"):
        _validate_step_range((None,))

    # Invalid value types
    with pytest.raises(ValueError, match="start must be None or a number"):
        _validate_step_range(("0", None))

    with pytest.raises(ValueError, match="end must be None or a number"):
        _validate_step_range((None, "10"))

    # Invalid range
    with pytest.raises(ValueError, match="start must be less than or equal to end"):
        _validate_step_range((10, 0))


def test_validate_tail_limit():
    # Valid cases
    _validate_tail_limit(None)
    _validate_tail_limit(1)
    _validate_tail_limit(100)

    # Invalid cases
    with pytest.raises(ValueError, match="must be None or an integer"):
        _validate_tail_limit(1.5)

    with pytest.raises(ValueError, match="must be None or an integer"):
        _validate_tail_limit("1")

    with pytest.raises(ValueError, match="must be greater than 0"):
        _validate_tail_limit(0)

    with pytest.raises(ValueError, match="must be greater than 0"):
        _validate_tail_limit(-1)


def test_validate_include_time():
    # Valid cases
    _validate_include_time(None)
    _validate_include_time("absolute")


def test_validate_include_time_invalid():
    # Invalid cases
    with pytest.raises(ValueError, match="include_time must be 'absolute'"):
        _validate_include_time("invalid")

    with pytest.raises(ValueError, match="include_time must be 'absolute'"):
        _validate_include_time("relative")
