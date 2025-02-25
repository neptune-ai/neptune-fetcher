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
from pandas._testing import assert_frame_equal

from neptune_fetcher.alpha.exceptions import ConflictingAttributeTypes
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.output_format import (
    convert_table_to_dataframe,
    create_dataframe,
)
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.attribute_types import FloatSeriesAggregations
from neptune_fetcher.alpha.internal.retrieval.attribute_values import AttributeValue
from neptune_fetcher.alpha.internal.retrieval.metrics import FloatPointValue

EXPERIMENT_IDENTIFIER = identifiers.RunIdentifier(
    identifiers.ProjectIdentifier("project/abc"), identifiers.SysId("XXX-1")
)


def test_convert_experiment_table_to_dataframe_empty():
    # given
    experiment_data = {}

    # when
    dataframe = convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=False)

    # then
    assert dataframe.empty


def test_convert_experiment_table_to_dataframe_single_string():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=False)

    # then
    assert dataframe.to_dict() == {
        ("attr1", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_string_with_type_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=True)

    # then
    assert dataframe.to_dict() == {
        ("attr1:int", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_float_series():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "float_series"),
                FloatSeriesAggregations(last=42.0, min=0.0, max=100, average=24.0, variance=100.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(
        experiment_data,
        selected_aggregations={
            AttributeDefinition("attr1", "float_series"): {"last", "min", "variance"},
        },
        type_suffix_in_column_names=False,
    )

    # then
    assert dataframe.to_dict() == {
        ("attr1", "last"): {"exp1": 42.0},
        ("attr1", "min"): {"exp1": 0.0},
        ("attr1", "variance"): {"exp1": 100.0},
    }


def test_convert_experiment_table_to_dataframe_disjoint_names():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr2", "int"), 43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=False)

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            ("attr1", ""): {"exp1": 42.0, "exp2": float("nan")},
            ("attr2", ""): {"exp1": float("nan"), "exp2": 43.0},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns = pd.MultiIndex.from_tuples(expected_data.columns, names=["attribute", "aggregation"])
    assert_frame_equal(dataframe, expected_data)


def test_convert_experiment_table_to_dataframe_conflicting_types_with_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=True)

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            ("attr1/a:b:c:float", ""): {"exp1": float("nan"), "exp2": 0.43},
            ("attr1/a:b:c:int", ""): {"exp1": 42.0, "exp2": float("nan")},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns = pd.MultiIndex.from_tuples(expected_data.columns, names=["attribute", "aggregation"])
    assert_frame_equal(dataframe, expected_data)


def test_convert_experiment_table_to_dataframe_conflicting_types_without_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    with pytest.raises(ConflictingAttributeTypes) as exc_info:
        convert_table_to_dataframe(experiment_data, selected_aggregations={}, type_suffix_in_column_names=False)

    # then
    assert "attr1/a:b:c" in str(exc_info.value)


EXPERIMENTS = 5
PATHS = 5
STEPS = 10


def _generate_float_point_values(
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
def test_create_dataframe_shape(include_preview):
    float_point_values = list(_generate_float_point_values(EXPERIMENTS, PATHS, STEPS, include_preview))

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
    path: str, type_suffix_in_column_names: bool, include_preview: bool, timestamp_column_name: str
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
def test_create_dataframe_with_reserved_paths_with_flat_index(path: str, type_suffix_in_column_names: bool):
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
