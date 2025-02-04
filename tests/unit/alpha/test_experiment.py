import pytest

from neptune_fetcher.alpha.experiment import (
    _validate_limit,
    _validate_sort_direction,
)


def test_validate_limit():
    # Valid cases
    _validate_limit(None)

    _validate_limit(1)
    _validate_limit(100)

    # Invalid cases
    with pytest.raises(ValueError, match="must be None or an integer"):
        _validate_limit(1.5)

    with pytest.raises(ValueError, match="must be None or an integer"):
        _validate_limit("1")

    with pytest.raises(ValueError, match="must be greater than 0"):
        _validate_limit(0)

    with pytest.raises(ValueError, match="must be greater than 0"):
        _validate_limit(-1)


def test_validate_sort_direction():
    # Valid cases
    _validate_sort_direction("asc")
    _validate_sort_direction("desc")

    # Invalid cases
    with pytest.raises(ValueError, match="must be either 'asc' or 'desc'"):
        _validate_sort_direction("invalid")
