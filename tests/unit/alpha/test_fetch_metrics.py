from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from neptune_fetcher.alpha.internal.composition.fetch_metrics import (
    _transform_with_absolute_timestamp,
    _transform_without_timestamp,
    _validate_include_time,
    _validate_step_range,
    _validate_tail_limit,
)


def _format_path_name(path: str, type_suffix_in_column_names: bool) -> str:
    return f"{path}:float_series" if type_suffix_in_column_names else path


@pytest.mark.parametrize("type_suffix_in_column_names,include_preview", [[True, False], [True, False]])
def test_transform_with_absolute_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = {
        "experiment": ["exp1", "exp1", "exp2"],
        "step": [1, 2, 1],
        "path": ["path1", "path2", "path1"],
        "value": [10.0, 20.0, 30.0],
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
    }
    if include_preview:
        data["is_preview"] = [False, False, True]
        data["preview_completion"] = [1.0, 1.0, 0.5]

    df = pd.DataFrame(data)
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_with_absolute_timestamp(
        df, include_point_previews=include_preview, type_suffix_in_column_names=type_suffix_in_column_names
    )

    # Then
    expected = {
        (_format_path_name("path1", type_suffix_in_column_names), "absolute_time"): [
            datetime(2023, 1, 1),
            np.nan,
            datetime(2023, 1, 3),
        ],
        (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
        (_format_path_name("path2", type_suffix_in_column_names), "absolute_time"): [
            np.nan,
            datetime(2023, 1, 2),
            np.nan,
        ],
        (_format_path_name("path2", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
    }
    if include_preview:
        expected.update(
            {
                (_format_path_name("path1", type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
                (_format_path_name("path1", type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
                (_format_path_name("path2", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
                (_format_path_name("path2", type_suffix_in_column_names), "preview_completion"): [np.nan, 1.0, np.nan],
            }
        )
    expected_df = pd.DataFrame(
        expected,
        index=pd.MultiIndex.from_tuples([("exp1", 1), ("exp1", 2), ("exp2", 1)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=False)


@pytest.mark.parametrize("type_suffix_in_column_names,include_preview", [[True, False], [True, False]])
def test_transform_without_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = {
        "experiment": ["exp1", "exp1", "exp2"],
        "step": [1, 2, 1],
        "path": ["path1", "path2", "path1"],
        "value": [10.0, 20.0, 30.0],
    }
    if include_preview:
        data["is_preview"] = [False, False, True]
        data["preview_completion"] = [1.0, 1.0, 0.5]

    df = pd.DataFrame(data)
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_without_timestamp(
        df, include_point_previews=include_preview, type_suffix_in_column_names=type_suffix_in_column_names
    )

    # Then
    expected = {
        "experiment": ["exp1", "exp1", "exp2"],
        "step": [1, 2, 1],
    }
    if include_preview:
        expected.update(
            {
                (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
                (_format_path_name("path1", type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
                (_format_path_name("path1", type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
                (_format_path_name("path2", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
                (_format_path_name("path2", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
                (_format_path_name("path2", type_suffix_in_column_names), "preview_completion"): [np.nan, 1.0, np.nan],
            }
        )
    else:
        expected.update(
            {
                _format_path_name("path1", type_suffix_in_column_names): [10.0, np.nan, 30.0],
                _format_path_name("path2", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
            }
        )

    expected_df = pd.DataFrame(expected).set_index(["experiment", "step"])

    print(f"comp: \n{transformed_df} \n {expected_df}\n\n\n\n")

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=False)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_transform_empty_dataframe_without_timestamp(type_suffix_in_column_names: bool):
    # Given empty dataframe
    df = pd.DataFrame(
        {
            "experiment": [],
            "timestamp": [],
            "step": [],
            "path": [],
            "value": [],
        }
    )
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_without_timestamp(
        df, include_point_previews=False, type_suffix_in_column_names=type_suffix_in_column_names
    )
    # Then
    expected_df = pd.DataFrame(
        {
            "experiment": [],
            "step": [],
        }
    ).set_index(["experiment", "step"])

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_index_type=False)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_transform_empty_dataframe_with_timestamp(type_suffix_in_column_names: bool):
    # Given empty dataframe
    df = pd.DataFrame(
        {
            "experiment": [],
            "timestamp": [],
            "step": [],
            "path": [],
            "value": [],
        }
    )
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_with_absolute_timestamp(
        df, include_point_previews=False, type_suffix_in_column_names=type_suffix_in_column_names
    )

    # Then
    expected_df = pd.DataFrame(
        index=pd.MultiIndex.from_tuples([], names=["experiment", "step"]),
        columns=pd.MultiIndex.from_tuples([], names=["path", "metric"]),  # Create empty MultiIndex for columns
    )
    expected_df.columns.names = None, None
    pd.testing.assert_frame_equal(transformed_df, expected_df, check_index_type=False, check_column_type=False)


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
