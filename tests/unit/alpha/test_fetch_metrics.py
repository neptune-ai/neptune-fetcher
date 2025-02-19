import itertools
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Generator

import numpy as np
import pandas as pd
import pytest

from neptune_fetcher.alpha.internal.composition.fetch_metrics import (
    _validate_include_time,
    _validate_step_range,
    _validate_tail_limit,
)
from neptune_fetcher.alpha.internal.output_format import create_dataframe
from neptune_fetcher.alpha.internal.retrieval.metrics import FloatPointValue

EXPERIMENTS = 5
PATHS = 5
STEPS = 10


def generate_float_point_values(
    experiments: int, paths: int, steps: int, preview: bool
) -> Generator[FloatPointValue, None, None]:
    for experiment in range(experiments):
        for path in range(paths):
            for step in range(steps):
                timestamp = datetime(2023, 1, 1, 0, 0, 0, 0, timezone.utc) + timedelta(seconds=step)
                yield (
                    f"exp{experiment}",
                    f"path{path}",
                    timestamp.timestamp(),
                    float(step),
                    float(step) * 100,
                    preview,
                    1.0 - (float(step) / 1000.0),
                )


def _format_path_name(path: str, type_suffix_in_column_names: bool) -> str:
    return f"{path}:float_series" if type_suffix_in_column_names else path


def _make_timestamp(year: int, month: int, day: int) -> float:
    return datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000


@pytest.mark.parametrize("include_preview", [False, True])
def test_create_flat_dataframe_shape(include_preview):
    float_point_values = list(generate_float_point_values(EXPERIMENTS, PATHS, STEPS, include_preview))

    """Test the creation of a flat DataFrame from float point values."""
    df = create_dataframe(
        float_point_values,
        include_point_previews=include_preview,
        type_suffix_in_column_names=False,
        index_column_name="experiment",
    )

    # Check if the DataFrame is not empty
    assert not df.empty, "DataFrame should not be empty"

    # Check the shape of the DataFrame
    num_expected_rows = EXPERIMENTS * STEPS
    assert df.shape[0] == num_expected_rows, f"DataFrame should have {num_expected_rows} rows"

    # Check the columns of the DataFrame
    all_paths = set(fp[1] for fp in float_point_values)
    if not include_preview:
        expected_columns = all_paths
    else:
        expected_columns = set(itertools.product(all_paths, ["value", "is_preview", "preview_completion"]))

    assert set(df.columns) == expected_columns, f"DataFrame should have {len(all_paths)} columns"
    assert (
        df.index.get_level_values(0).nunique() == EXPERIMENTS
    ), f"DataFrame should have {EXPERIMENTS} experiment names"

    # Convert DataFrame to list of tuples
    tuples_list = list(df.to_records(index=False))
    assert (
        len(tuples_list) == num_expected_rows
    ), "The list of tuples should have the same number of rows as the DataFrame"


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
def test_create_dataframe_with_absolute_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = [
        ("exp1", "path1", _make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ("exp1", "path2", _make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ("exp2", "path1", _make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
    ]

    df = create_dataframe(
        data,
        timestamp_column_name="absolute_time",
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    expected = {
        (_format_path_name("path1", type_suffix_in_column_names), "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
        (_format_path_name("path2", type_suffix_in_column_names), "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
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
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
def test_create_dataframe_without_timestamp(type_suffix_in_column_names: bool, include_preview: bool):
    # Given
    data = [
        ("exp1", "path1", _make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ("exp1", "path2", _make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ("exp2", "path1", _make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
    ]

    df = create_dataframe(
        data,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    if not include_preview:
        # Flat columns
        expected = {
            _format_path_name("path1", type_suffix_in_column_names): [10.0, np.nan, 30.0],
            _format_path_name("path2", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
        }
    else:
        # MultiIndex columns are returned on include_preview=True
        expected = {
            (_format_path_name("path1", type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
            (_format_path_name("path2", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
            (_format_path_name("path1", type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
            (_format_path_name("path1", type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
            (_format_path_name("path2", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
            (_format_path_name("path2", type_suffix_in_column_names), "preview_completion"): [np.nan, 1.0, np.nan],
        }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
@pytest.mark.parametrize("timestamp_column_name", [None, "absolute"])
def test_create_empty_dataframe(type_suffix_in_column_names: bool, include_preview: bool, timestamp_column_name: str):
    # Given empty dataframe

    # When
    df = create_dataframe(
        [],
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        timestamp_column_name=timestamp_column_name,
        index_column_name="experiment",
    )

    # Then
    if include_preview or timestamp_column_name:
        expected_df = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([], names=["experiment", "step"]),
            columns=pd.MultiIndex.from_tuples([], names=["path", "metric"]),  # Create empty MultiIndex for columns
        )
        expected_df.columns.names = None, None
    else:
        expected_df = pd.DataFrame(
            {
                "experiment": [],
                "step": [],
            }
        ).set_index(["experiment", "step"])

    pd.testing.assert_frame_equal(df, expected_df, check_index_type=False)


@pytest.mark.parametrize(
    "path", ["value", "step", "experiment", "value", "timestamp", "is_preview", "preview_completion"]
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_preview", [True, False])
@pytest.mark.parametrize("timestamp_column_name", ["absolute_time"])
def test_create_dataframe_with_reserved_paths_with_multiindex(
    path, type_suffix_in_column_names: bool, include_preview: bool, timestamp_column_name: str
):
    # Given
    data = [
        ("exp1", path, _make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ("exp1", "other_path", _make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ("exp2", path, _make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
    ]

    df = create_dataframe(
        data,
        timestamp_column_name=timestamp_column_name,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_preview,
        index_column_name="experiment",
    )

    # Then
    expected = {
        (_format_path_name(path, type_suffix_in_column_names), "absolute_time"): [
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            np.nan,
            datetime(2023, 1, 2, tzinfo=timezone.utc),
        ],
        (_format_path_name(path, type_suffix_in_column_names), "value"): [10.0, np.nan, 30.0],
        (_format_path_name("other_path", type_suffix_in_column_names), "absolute_time"): [
            np.nan,
            datetime(2023, 1, 3, tzinfo=timezone.utc),
            np.nan,
        ],
        (_format_path_name("other_path", type_suffix_in_column_names), "value"): [np.nan, 20.0, np.nan],
    }
    if include_preview:
        expected.update(
            {
                (_format_path_name(path, type_suffix_in_column_names), "is_preview"): [False, np.nan, True],
                (_format_path_name(path, type_suffix_in_column_names), "preview_completion"): [1.0, np.nan, 0.5],
                (_format_path_name("other_path", type_suffix_in_column_names), "is_preview"): [np.nan, False, np.nan],
                (_format_path_name("other_path", type_suffix_in_column_names), "preview_completion"): [
                    np.nan,
                    1.0,
                    np.nan,
                ],
            }
        )

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "path", ["value", "step", "experiment", "value", "timestamp", "is_preview", "preview_completion"]
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_create_dataframe_with_reserved_paths_with_flat_index(path, type_suffix_in_column_names: bool):
    # Given
    data = [
        ("exp1", path, _make_timestamp(2023, 1, 1), 1, 10.0, False, 1.0),
        ("exp1", "other_path", _make_timestamp(2023, 1, 3), 2, 20.0, False, 1.0),
        ("exp2", path, _make_timestamp(2023, 1, 2), 1, 30.0, True, 0.5),
    ]

    df = create_dataframe(
        data,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        index_column_name="experiment",
    )

    # Then
    expected = {
        _format_path_name(path, type_suffix_in_column_names): [10.0, np.nan, 30.0],
        _format_path_name("other_path", type_suffix_in_column_names): [np.nan, 20.0, np.nan],
    }

    expected_df = pd.DataFrame(
        dict(sorted(expected.items())),
        index=pd.MultiIndex.from_tuples([("exp1", 1.0), ("exp1", 2.0), ("exp2", 1.0)], names=["experiment", "step"]),
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


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
