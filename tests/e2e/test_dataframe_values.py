import os
import re
import time

import pytest

from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


@pytest.mark.parametrize("limit", [1, 2, 6, 12, 1000])
def test__all_runs_limit(project, all_run_ids, all_experiment_ids, sys_columns, limit):
    df = project.fetch_runs_df(limit=limit, columns=sys_columns)
    expect = min(limit, len(all_run_ids + all_experiment_ids))
    assert len(df) == expect


@pytest.mark.parametrize("limit", [1, 2, 6, 1000])
def test__all_experiments_limit(project, all_experiment_ids, sys_columns, limit):
    df = project.fetch_experiments_df(limit=limit, columns=sys_columns)
    expect = min(limit, len(all_experiment_ids))
    assert len(df) == expect


def test__too_high_limit_raises_error(project):
    with pytest.raises(ValueError, match="limit.*can't be higher than.*"):
        project.fetch_runs_df(limit=1000000)


@pytest.mark.parametrize("limit", [0, -1, -12345])
def test__invalid_limit_raises_error(project, limit):
    with pytest.raises(ValueError, match="limit.*greater than 0"):
        project.fetch_runs_df(limit=limit)

    with pytest.raises(ValueError, match="limit.*greater than 0"):
        project.fetch_experiments_df(limit=limit)


def test__nonexistent_column_is_returned_as_null(project, sys_columns):
    df = project.fetch_runs_df(columns=["non_existent_column"] + sys_columns)
    assert df["non_existent_column"].isnull().all()


def test__nonexistent_column_in_prefetch(project: ReadOnlyProject, sys_columns):
    run_id = next(project.list_experiments())["sys/id"]
    run = ReadOnlyRun(project, with_id=run_id)
    fields = list(run.field_names)

    non_existent_column = f"non_existent_column/{time.time()}"
    fields.append(non_existent_column)

    run.prefetch_series_values(fields)

    with pytest.raises(KeyError):
        run[non_existent_column].fetch_values()


@pytest.mark.parametrize(
    # The expected count doesn't include the default columns. It's adjusted in the test code.
    "regex, run_ids, expect_count",
    [
        (r"\Donfig/.*unique-id-run-2", None, 10),
        ("config/.*unique-id-.*-1", None, 20),
        (".*unique-id-run-2", None, 20),  # 10 unique config/* and metrics/*
        (r"metrics/.*|config/.*\d$", ["id-run-5"], 60),  # All columns
        ("non_existent_column", None, 0),
    ],
)
def test__column_regex(project, regex, run_ids, expect_count):
    """Request columns using a regex, keeping in mind that we always return custom_run_id, name, and the sorting
    column"""
    df = project.fetch_runs_df(columns_regex=regex, custom_ids=run_ids)
    assert len(df.columns) == expect_count + 3  # account for the default columns


def test__columns_regex_and_list_together(project, sys_columns):
    """Request both specific columns and columns_regex. The returned set must strictly match."""

    # Both sys attributes and metrics should be returned
    df = project.fetch_runs_df(
        columns_regex="metrics/foo.+", columns=sys_columns, sort_by="sys/id", custom_ids=["id-run-3"]
    )
    assert set(df.columns) == set(sys_columns + [f"metrics/foo{x}" for x in range(1, 11)])

    # Passing a strict column name should return only that column even if
    # we provide additional regex
    df = project.fetch_runs_df(columns=["config/foo1"], columns_regex="sys/")
    assert sum(not col.startswith("sys/") for col in df.columns) == 1, "There should be only 1 non-sys column returned"


def test__columns_not_present_in_all_runs_are_null(project, sys_columns):
    """
    Request columns that exist only in some runs while explicitly requesting runs by ID.
    The columns that don't exist in a run should be returned as nulls
    """

    # The requested column is unique for id-run-10
    df = project.fetch_runs_df(
        columns=["config/foo1-unique-id-run-1"],
        custom_ids=["id-run-3", "id-run-1"],
        sort_by="sys/custom_run_id",
        ascending=True,
    )

    assert len(df) == 2
    assert len(df.columns) == 3  # sys/custom_run_id and sys/name are always included
    assert df["config/foo1-unique-id-run-1"].isnull().sum() == 1  # Only id-run-10 has this column


def test__columns_present_in_all_experiments(project, all_experiment_ids):
    """Fetch columns that we know are part of all experiments"""

    df = project.fetch_experiments_df(columns_regex="metrics/foo.*", sort_by="sys/custom_run_id")
    assert len(df) == len(all_experiment_ids)
    # 10 metrics/foo* per experiment + the sorting column + sys/name for experiments
    assert df.count().sum() == len(all_experiment_ids) * 12


def test__columns_must_match_runs(project, all_run_ids):
    """
    Check if the returned columns match their expected rows (runs).
    Columns that don't exist in a run should be null.
    """

    ids = all_run_ids[:3]

    # Query just the unique columns. For each column, only a single run can have a non-null value.
    df = project.fetch_runs_df(
        columns_regex="config/.*9.*unique.*", custom_ids=ids, sort_by="sys/custom_run_id", ascending=True
    )

    assert len(df) == len(ids)

    # Make sure non-null columns actually match the run IDs. There should be a single non-null value in each column.
    # The column name should contain the run ID, as this is how the data is laid out by populate_projects.py.
    for col in df.columns:
        if col.startswith("sys/"):
            continue

        assert df[col].notna().sum() == 1, f"Column {col} must have only one non-null value"

        # Index of a row with a non-null value in the column
        row_with_value = df[col].notna().idxmax()
        # Check if the column name contains the run ID.
        assert col.endswith(ids[row_with_value]), f"Column {col} must match the run ID {ids[row_with_value]}"


def test__default_columns(project):
    """We should always return sys/custom_run_id as well as the sorting column. The columns must be unique."""

    df = project.fetch_runs_df(columns=["no-such-column"])
    assert set(df.columns) == {"sys/name", "sys/custom_run_id", "sys/creation_time", "no-such-column"}

    df = project.fetch_runs_df(sort_by="sys/name", columns=["no-such-column"])
    assert set(df.columns) == {"sys/custom_run_id", "sys/name", "no-such-column"}

    # Experiments always include sys/name next to the other default column
    df = project.fetch_experiments_df(columns_regex="^no-such-column")
    assert set(df.columns) == {"sys/custom_run_id", "sys/name", "sys/creation_time"}


@pytest.mark.parametrize("use_prefetch", [True, False], ids=["with_prefetch", "without_prefetch"])
def test__metrics_preview(project, all_run_ids, use_prefetch):
    run = ReadOnlyRun(project, custom_id=all_run_ids[0])
    if use_prefetch:
        run.prefetch_series_values(paths=["metrics/foo1"])

    with_preview_values = run["metrics/foo1"].fetch_values(include_preview=True)
    values = run["metrics/foo1"].fetch_values(include_preview=False)

    assert len(with_preview_values) == 13
    assert len(values) == 10

    assert with_preview_values["step"].tolist() == [float(i) for i in range(13)]
    assert values["step"].tolist() == [float(i) for i in range(10)]

    assert with_preview_values["preview"].tolist() == [False for _ in range(10)] + [True for _ in range(3)]
    assert with_preview_values["completion_ratio"].tolist() == [1.0] * 10 + [
        pytest.approx(0.10, 0.01),
        pytest.approx(0.11, 0.01),
        pytest.approx(0.12, 0.01),
    ]

    assert with_preview_values.columns.tolist() == ["step", "value", "timestamp", "preview", "completion_ratio"]
    assert values.columns.tolist() == ["step", "value", "timestamp"]


def _validate_sys_attr(row, index, column, value):
    if column == "sys/name":
        if isinstance(value, str):
            assert index <= 6, "Experiment name must be present only for experiments"
            assert value == f"exp{index}"
        else:
            assert index >= 6, "Experiment name must be present only for experiments"
    else:
        assert row[column] is not None, f"Column {column} must not be null"

    if column == "sys/custom_run_id":
        if isinstance(row["sys/name"], str):
            assert value == f"id-exp-{index}"
        else:
            assert value == f"id-run-{index - 6}"  # Account for the first 6 rows being experiments


# Regex patterns to extract the expected integer value given a column name
INT_COLUMNS = (
    re.compile(r"config/bar(\d+)$"),
    re.compile(r"metrics/foo(\d+)$"),
    re.compile(r"metrics/bar(\d+)$"),
)


def expected_int_value(column_name):
    for regex in INT_COLUMNS:
        if match := regex.match(column_name):
            return int(match.group(1))

    assert False, "No expected column was matched"


def test__full_data_consistency(project, all_run_ids, all_experiment_ids):
    """
    Test all columns for all runs and experiments. The data must be consistent
    with the assumptions made in populate_projects.py.
    This test is quite heavy because of the amount of data loaded.
    """

    # Additional assumptions:
    #  - we sort by custom_run_id, so the first 6 rows are experiments, then runs
    #  - because of that we can assume that i >= 6 in the loop below means an experiment
    df = project.fetch_runs_df(sort_by="sys/custom_run_id", columns_regex=".*", ascending=True)

    # All runs should be present
    assert len(df) == len(all_run_ids + all_experiment_ids)
    # All columns that are not system columns: 10 * config/* + 10 * metrics/* + 20 unique metrics * 12 runs
    assert len([col for col in df.columns if not col.startswith("sys/")]) == 280

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        for column in df.columns[1:]:
            value = row[column]

            if column.startswith("sys/"):
                _validate_sys_attr(row, i, column, value)
            elif "unique" in column:
                if isinstance(value, str):
                    # Column format: "config/fooX-unique-CUSTOM_RUN_ID", custom run id must match
                    assert column.endswith(row["sys/custom_run_id"])
            elif column.startswith("config/foo"):
                assert value == f"valfoo{column.replace('config/foo', '')}"
            else:
                # Any other column must be an integer column
                assert value == expected_int_value(column), f"Column {column} does not have the expected value"
