import pytest


@pytest.mark.parametrize("limit", [1, 2, 20, 100, 1000])
def test__all_runs_limit(project, all_run_ids, all_experiment_ids, sys_columns, limit):
    df = project.fetch_runs_df(limit=limit, columns=sys_columns)
    expect = min(limit, len(all_run_ids + all_experiment_ids))
    assert len(df) == expect


@pytest.mark.parametrize("limit", [1, 2, 10, 100, 1000])
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


@pytest.mark.parametrize(
    # The expected count doesn't include the default columns. It's adjusted in the test code.
    "regex, run_ids, expect_count",
    [
        ("config/.*unique-id-run-2", None, 100),
        ("config/.*unique-id-run-1.*", None, 200),
        (".*unique-id-run-2", None, 200),  # 100 unique config/* and metrics/*
        ("metrics/.*|config/.*", ["id-run-5"], 40200),  # All columns in a project
        ("non_existent_column", None, 0),
    ],
)
def test__column_regex(project, regex, run_ids, expect_count):
    """Request columns using a regex, keeping in mind that we always return custom_run_id and the sorting column"""
    df = project.fetch_runs_df(columns_regex=regex, custom_ids=run_ids)
    assert len(df.columns) == expect_count + 2


def test__columns_regex_and_list_together(project, sys_columns):
    """Request both specific columns and columns_regex. The returned set must strictly match."""

    # Both sys attributes and metrics should be returned
    df = project.fetch_runs_df(
        columns_regex="metrics/foo999.+", columns=sys_columns, sort_by="sys/id", custom_ids=["id-run-3"]
    )
    assert set(df.columns) == set(sys_columns + [f"metrics/foo999{x}" for x in range(10)])

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
        columns=["config/foo1-unique-id-run-10"],
        custom_ids=["id-run-3", "id-run-10"],
        sort_by="sys/custom_run_id",
        ascending=True,
    )

    assert len(df) == 2
    assert len(df.columns) == 2  # sys/custom_run_id is always included
    assert df["config/foo1-unique-id-run-10"].isnull().sum() == 1  # Only id-run-10 has this column


def test__columns_present_in_all_experiments(project, all_experiment_ids):
    """Fetch columns that we know are part of all experiments"""

    df = project.fetch_experiments_df(columns_regex="metrics/foo.*", sort_by="sys/custom_run_id")
    assert len(df) == len(all_experiment_ids)
    # 10000 metrics/foo* per experiment + the sorting column + sys/name for experiments
    assert df.count().sum() == len(all_experiment_ids) * 10002


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
    assert set(df.columns) == {"sys/custom_run_id", "sys/creation_time", "no-such-column"}

    df = project.fetch_runs_df(sort_by="sys/name", columns=["no-such-column"])
    assert set(df.columns) == {"sys/custom_run_id", "sys/name", "no-such-column"}

    # Experiments always include sys/name next to the other default column
    df = project.fetch_experiments_df(columns_regex="^no-such-column")
    assert set(df.columns) == {"sys/custom_run_id", "sys/name", "sys/creation_time"}
