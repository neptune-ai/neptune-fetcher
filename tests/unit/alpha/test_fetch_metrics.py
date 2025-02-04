from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from neptune_fetcher.alpha.fetch_metrics import (
    _transform_with_absolute_timestamp,
    _transform_without_timestamp,
)


def _format_path_name(path: str, type_suffix_in_column_names: bool) -> str:
    return f"{path}:float_series" if type_suffix_in_column_names else path


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_transform_with_absolute_timestamp(type_suffix_in_column_names: bool):
    # Given
    df = pd.DataFrame(
        {
            "experiment": ["exp1", "exp1", "exp2"],
            "step": [1, 2, 1],
            "path": ["path1", "path2", "path1"],
            "value": [10.0, 20.0, 30.0],
            "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)],
        }
    )
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_with_absolute_timestamp(df, type_suffix_in_column_names=type_suffix_in_column_names)

    # Then
    expected_df = pd.DataFrame(
        {
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
        },
        index=pd.MultiIndex.from_tuples([("exp1", 1), ("exp1", 2), ("exp2", 1)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=False)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_transform_without_timestamp(type_suffix_in_column_names: bool):
    # Given
    df = pd.DataFrame(
        {
            "experiment": ["exp1", "exp1", "exp2"],
            "step": [1, 2, 1],
            "path": ["path1", "path2", "path1"],
            "value": [10.0, 20.0, 30.0],
        }
    )
    df["experiment"] = df["experiment"].astype("category")
    df["path"] = df["path"].astype("category")

    # When
    transformed_df = _transform_without_timestamp(df, type_suffix_in_column_names=type_suffix_in_column_names)

    # Then
    expected_df = pd.DataFrame(
        {
            "experiment": ["exp1", "exp1", "exp2"],
            "step": [1, 2, 1],
            _format_path_name("path1", type_suffix_in_column_names): [10.0, np.nan, 30.0],
            _format_path_name("path2", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
        }
    ).set_index(["experiment", "step"])

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
    transformed_df = _transform_without_timestamp(df, type_suffix_in_column_names=type_suffix_in_column_names)
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
    transformed_df = _transform_with_absolute_timestamp(df, type_suffix_in_column_names=type_suffix_in_column_names)

    # Then
    expected_df = pd.DataFrame(
        index=pd.MultiIndex.from_tuples([], names=["experiment", "step"]),
        columns=pd.MultiIndex.from_tuples([], names=["path", "metric"]),  # Create empty MultiIndex for columns
    )
    expected_df.columns.names = None, None
    pd.testing.assert_frame_equal(transformed_df, expected_df, check_index_type=False, check_column_type=False)
