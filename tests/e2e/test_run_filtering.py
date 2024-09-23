import pandas as pd
import pytest

#
# Tests for filtering runs by various attributes
#


def shortid(num):
    """
    Generate an existing short ID for a run/experiment. The function assumes a specific project key.
    """
    return f"MANY-{num}"


def test__runs_no_filter(project, all_run_ids, all_experiment_ids, sys_columns):
    """Requesting all runs without any filter should return all runs and experiments"""

    df = project.fetch_runs_df(columns=sys_columns, sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == sorted(all_run_ids + all_experiment_ids)


def test__experiments_no_filter(project, all_experiment_ids, all_experiment_names, sys_columns):
    df = project.fetch_experiments_df(
        custom_id_regex="id-exp.*", columns=sys_columns, sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == all_experiment_ids
    assert df["sys/name"].tolist() == all_experiment_names


@pytest.mark.parametrize(
    "regex, expected",
    [
        ("id-run-1", ["id-run-1", "id-run-10"]),  # This defaults to .*id-run-1.* in the backend
        ("id-run-1.*", ["id-run-1", "id-run-10"]),
        ("^id-run-1$", ["id-run-1"]),
        ("id-run-2", ["id-run-2"]),
        ("nonexistent", []),
    ],
)
def test__runs_by_custom_id_regex(project, sys_columns, regex, expected):
    df = project.fetch_runs_df(custom_id_regex=regex, columns=sys_columns, sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == sorted(expected)
    assert df["sys/name"].isna().all(), "Run name must be empty"


@pytest.mark.parametrize(
    "regex, expect_ids, expect_names",
    [
        ("id-exp-1", ["id-exp-1", "id-exp-10"], ["exp1", "exp10"]),  # This equivalent to .*id-exp-1.*
        ("id-exp-1.*", ["id-exp-1", "id-exp-10"], ["exp1", "exp10"]),
        ("^id-exp-1$", ["id-exp-1"], ["exp1"]),
        ("id-exp-2", ["id-exp-2"], ["exp2"]),
        ("nonexistent", [], []),
        ("id-run.*", [], []),  # Runs shouldn't be returned when requesting experiments
    ],
)
def test__experiments_by_custom_id_regex(project, sys_columns, regex, expect_ids, expect_names):
    df = project.fetch_experiments_df(
        custom_id_regex=regex, columns=sys_columns, sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids
    assert df["sys/name"].tolist() == expect_names


@pytest.mark.parametrize(
    "short_id, expect_ids, expect_names",
    [
        ([shortid(1)], ["id-run-1"], [pd.NA]),
        ([shortid(2), shortid(3)], ["id-run-2", "id-run-3"], [pd.NA, pd.NA]),
        ([shortid(15)], ["id-exp-5"], ["exp5"]),  # Experiments are also runs
        ("nonexistent", [], []),
    ],
)
def test__runs_by_short_id(project, sys_columns, short_id, expect_ids, expect_names):
    df = project.fetch_runs_df(with_ids=short_id, columns=sys_columns, sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == expect_ids
    assert df["sys/name"].tolist() == expect_names


@pytest.mark.parametrize(
    "short_id, expect_ids, expect_names",
    [
        ([shortid(15)], ["id-exp-5"], ["exp5"]),
        ([shortid(11), shortid(12)], ["id-exp-1", "id-exp-2"], ["exp1", "exp2"]),
        ([shortid(1)], [], []),  # This is a run, so it shouldn't be returned
        ("nonexistent", [], []),
    ],
)
def test__experiments_by_short_id(project, sys_columns, short_id, expect_ids, expect_names):
    df = project.fetch_experiments_df(
        with_ids=short_id, columns=sys_columns, sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids
    assert df["sys/name"].tolist() == expect_names


@pytest.mark.parametrize(
    "names_regex, custom_id_regex, custom_ids, expect_ids",
    [
        ("exp1", None, None, ["id-exp-1", "id-exp-10"]),
        ("exp2", None, None, ["id-exp-2"]),
        ("exp1", "id-exp-1", None, ["id-exp-1", "id-exp-10"]),
        ("exp1", None, ["id-exp-1", "id-exp-10"], ["id-exp-1", "id-exp-10"]),
        ("exp2", "id-exp-1", None, []),  # Name and custom_id_regex don't match
        ("exp3", None, ["id-exp-5"], []),  # Name and custom_ids don't match
        (None, "id-run-1", None, []),  # Runs that are not experiments
        (None, "id-exp-1.*", ["id-exp-1", "id-exp-10"], ["id-exp-1", "id-exp-10"]),  # Regex and verbatim ids match
        (None, "id-exp-2", ["id-exp-8"], []),  # Regex and verbatim ids don't match
        ("exp5", "id-exp-2", ["id-exp-2"], []),  # Ids match but name doesn't
        ("exp2", "id-exp-2", ["id-exp-2"], ["id-exp-2"]),  # All three match
        ("exp1.*", "id-exp-1.*", ["id-exp-1", "id-exp-10"], ["id-exp-1", "id-exp-10"]),  # All three match
    ],
)
def test__experiments_by_names_and_custom_ids(
    project, sys_columns, names_regex, id_to_name, custom_id_regex, custom_ids, expect_ids
):
    """Various combinations of namex_regex, custom_id_regex and custom_ids"""

    df = project.fetch_experiments_df(
        columns=sys_columns,
        names_regex=names_regex,
        custom_id_regex=custom_id_regex,
        custom_ids=custom_ids,
        sort_by="sys/custom_run_id",
        ascending=True,
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids
    for _, row in df.iterrows():
        assert row["sys/name"] == id_to_name[row["sys/custom_run_id"]]


def test__experiments_name_regex_is_empty(project, sys_columns):
    with pytest.raises(ValueError, match="names_regex.*empty string"):
        project.fetch_experiments_df(columns=sys_columns, names_regex="")


@pytest.mark.parametrize(
    "tags, expect_ids",
    # Note that in test code we limit the number of results by providing a custom_id_regex="1[0]*"
    # to make it easier to specify testcases
    [
        (["head"], ["id-exp-1", "id-run-1"]),
        (["tail"], ["id-exp-10", "id-run-10"]),
        (["head", "tag1"], ["id-run-1"]),
        (["tail", "tag2"], ["id-exp-10"]),
        (["head", "tail"], []),
        (["does-not-exist"], []),
        (["does-not-exist", "head", "tail", "tag2"], []),
    ],
)
def test__some_runs_by_tags(project, sys_columns, tags, expect_ids):
    df = project.fetch_runs_df(
        columns=sys_columns, tags=tags, custom_id_regex="1[0]*", sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids


@pytest.mark.parametrize(
    "tags, expect_ids",
    [
        (["head"], ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5"]),
        (["tail"], ["id-exp-10", "id-exp-6", "id-exp-7", "id-exp-8", "id-exp-9"]),
        (["does-not-exist"], []),
        (["head", "does-not-exist"], []),
        (["head", "tail"], []),
        (["tag1"], []),  # Runs are tagged using tag1, not experiments
        (["head", "tag1"], []),  # Runs are tagged using tag1, not experiments
        (["head", "tag1", "tag2"], []),  # Runs are tagged using tag1, not experiments
    ],
)
def test__some_experiments_by_tags(project, sys_columns, tags, expect_ids):
    df = project.fetch_experiments_df(columns=sys_columns, tags=tags, sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == expect_ids


def test__all_runs_by_tags(project, sys_columns, all_run_ids):
    df = project.fetch_runs_df(columns=sys_columns, tags=["tag1"], sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == all_run_ids


def test__all_experiments_by_tags(project, sys_columns, all_experiment_ids):
    df = project.fetch_experiments_df(columns=sys_columns, tags=["tag2"], sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == all_experiment_ids


def test__custom_nql_query(project, all_run_ids, all_experiment_ids):
    # All runs & experiments have this value
    query = '(`config/foo1`:string = "valfoo1")'
    df = project.fetch_runs_df(columns=["config/foo1"], query=query)
    assert len(df) == len(all_run_ids + all_experiment_ids), "Not all runs returned"

    df = project.fetch_experiments_df(columns_regex="config/foo.*", query=query)
    assert len(df) == len(all_experiment_ids), "Not all experiments returned"
