from datetime import datetime

import numpy as np
import pandas as pd

from neptune_fetcher.alpha.fetch_metrics import (
    _transform_with_absolute_timestamp,
    _transform_without_timestamp,
)


def test_transform_with_absolute_timestamp():
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

    # When
    transformed_df = _transform_with_absolute_timestamp(df, type_suffix_in_column_names=True)

    # Then
    expected_df = pd.DataFrame(
        {
            ("path1:float_series", "absolute_time"): [datetime(2023, 1, 1), np.nan, datetime(2023, 1, 3)],
            ("path1:float_series", "value"): [10.0, np.nan, 30.0],
            ("path2:float_series", "absolute_time"): [np.nan, datetime(2023, 1, 2), np.nan],
            ("path2:float_series", "value"): [np.nan, 20.0, np.nan],
        },
        index=pd.MultiIndex.from_tuples([("exp1", 1), ("exp1", 2), ("exp2", 1)], names=["experiment", "step"]),
    ).reset_index()

    # Set the correct MultiIndex column names
    expected_df.columns = pd.MultiIndex.from_tuples(expected_df.columns, names=["path", None])

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=False)


def test_transform_without_timestamp():
    # Given
    df = pd.DataFrame(
        {
            "experiment": ["exp1", "exp1", "exp2"],
            "step": [1, 2, 1],
            "path": ["path1", "path2", "path1"],
            "value": [10.0, 20.0, 30.0],
        }
    )

    # When
    transformed_df = _transform_without_timestamp(df, type_suffix_in_column_names=True)

    # Then
    expected_df = pd.DataFrame(
        {
            "experiment": ["exp1", "exp1", "exp2"],
            "step": [1, 2, 1],
            "path1:float_series": [10.0, np.nan, 30.0],
            "path2:float_series": [np.nan, 20.0, np.nan],
        }
    )
    expected_df.columns.name = "path"

    pd.testing.assert_frame_equal(transformed_df, expected_df, check_dtype=False)
