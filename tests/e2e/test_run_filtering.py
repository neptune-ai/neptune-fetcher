import os
from operator import itemgetter

import pytest

from neptune_fetcher import ReadOnlyRun

#
# Tests for filtering runs by various attributes
#

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


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
        ("id-run-1", ["id-run-1"]),
        ("^id-run-1$", ["id-run-1"]),
        ("id-run-[2,3]", ["id-run-2", "id-run-3"]),
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
        ("id-exp-[23]", ["id-exp-2", "id-exp-3"], ["exp2", "exp3"]),
        ("id-exp-1.*", ["id-exp-1"], ["exp1"]),
        (r"id-exp\D1", ["id-exp-1"], ["exp1"]),
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


def test__runs_by_sys_id(project, sys_columns, all_run_ids, all_experiment_ids):
    runs = sorted(project.list_runs(), key=itemgetter("sys/custom_run_id"))
    # Take every 2nd run to test the actual filtering
    sys_ids = [run["sys/id"] for i, run in enumerate(runs) if i % 2 == 0]

    df = project.fetch_runs_df(columns=sys_columns, with_ids=sys_ids, sort_by="sys/custom_run_id", ascending=True)

    expect_ids = (all_experiment_ids + all_run_ids)[::2]
    assert df["sys/custom_run_id"].tolist() == expect_ids
    assert df["sys/id"].tolist() == sys_ids


def test__experiments_by_sys_id(project, sys_columns, all_experiment_ids):
    runs = sorted(project.list_experiments(), key=itemgetter("sys/custom_run_id"))
    # Take every 2nd experiment to test the actual filtering
    sys_ids = [run["sys/id"] for i, run in enumerate(runs) if i % 2 == 0]

    df = project.fetch_experiments_df(
        columns=sys_columns, with_ids=sys_ids, sort_by="sys/custom_run_id", ascending=True
    )

    expect_ids = all_experiment_ids[::2]
    assert df["sys/custom_run_id"].tolist() == expect_ids
    assert df["sys/id"].tolist() == sys_ids


@pytest.mark.parametrize(
    "regex, expect_ids",
    [
        (r"exp\d", ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"]),
        (r"exp\D", []),
        (r"exp[[:digit:]]", ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"]),
        (r"[^\d]1", ["id-exp-1"]),
        (r"[\D][23]", ["id-exp-2", "id-exp-3"]),
        (r"\x65.*1", ["id-exp-1"]),
        (r"expA?1", ["id-exp-1"]),
        (r'exp"foo"', []),
    ],
)
def test__experiments_by_name_regex(project, sys_columns, regex, expect_ids):
    df = project.fetch_experiments_df(
        columns=sys_columns, names_regex=regex, sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids


@pytest.mark.parametrize(
    "names_regex, custom_id_regex, custom_ids, expect_ids",
    [
        ("exp1", None, None, ["id-exp-1"]),
        ("exp2", None, None, ["id-exp-2"]),
        (r"exp\d", r"2", None, ["id-exp-2"]),
        ("exp2", "id-exp-1", None, []),  # Name and custom_id_regex don't match
        ("exp3", None, ["id-exp-5"], []),  # Name and custom_ids don't match
        (None, "id-run-1", None, []),  # Runs that are not experiments
        (None, "id-exp-[23]+", ["id-exp-2", "id-exp-3"], ["id-exp-2", "id-exp-3"]),  # Regex and verbatim ids match
        (None, "id-exp-2", ["id-exp-8"], []),  # Regex and verbatim ids don't match
        ("exp5", "id-exp-2", ["id-exp-2"], []),  # Ids match but name doesn't
        ("exp2", "id-exp-2", ["id-exp-2"], ["id-exp-2"]),  # All three match
        ("exp[23]+", "id-exp-[23]+", ["id-exp-2", "id-exp-3"], ["id-exp-2", "id-exp-3"]),  # All three match
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


def test__experiments_name_regex_neg_is_empty(project, sys_columns):
    with pytest.raises(ValueError, match="names_exclude_regex.*empty string"):
        project.fetch_experiments_df(columns=sys_columns, names_exclude_regex="")


@pytest.mark.parametrize(
    "value",
    [r"+\abc" "+\abc", "foobar", r"foo\+bar", r"foo\\+bar"],
)
def test__experiments_name_constructor(project, value):
    with pytest.raises(ValueError, match="No experiment found with name .*"):
        ReadOnlyRun(project, experiment_name=value)

    with pytest.raises(ValueError, match="No experiment found with Neptune ID .*"):
        ReadOnlyRun(project, with_id=value)


@pytest.mark.parametrize(
    "tags, expect_ids",
    # Note that in test code we limit the number of results by providing a custom_id_regex="-[15]+"
    # to make it easier to specify testcases
    [
        (["head"], ["id-exp-1", "id-run-1"]),
        (["tail"], ["id-exp-5", "id-run-5"]),
        (["head", "tag1"], ["id-run-1"]),
        (["tail", "tag2"], ["id-exp-5"]),
        (["head", "tail"], []),
        (["does-not-exist"], []),
        (["does-not-exist", "head", "tail", "tag2"], []),
    ],
)
def test__some_runs_by_tags(project, sys_columns, tags, expect_ids):
    df = project.fetch_runs_df(
        columns=sys_columns, tags=tags, custom_id_regex="-[15]+", sort_by="sys/custom_run_id", ascending=True
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids


@pytest.mark.parametrize(
    "tags, expect_ids",
    [
        (["head"], ["id-exp-1", "id-exp-2", "id-exp-3"]),
        (["tail"], ["id-exp-4", "id-exp-5", "id-exp-6"]),
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


@pytest.mark.parametrize(
    "query, expect_ids",
    (
        ('`sys/name`:string MATCHES "exp1"', ["id-exp-1"]),
        (
            r'`sys/name`:string MATCHES "exp\\d"',
            ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"],
        ),
        (r'`config/foo1`:string = "valfoo1" AND `sys/name`:string MATCHES "exp[23]"', ["id-exp-2", "id-exp-3"]),
    ),
)
def test__experiments_with_custom_nql_query(project, query, expect_ids):
    df = project.fetch_experiments_df(query=query, sort_by="sys/custom_run_id", ascending=True)
    assert df["sys/custom_run_id"].tolist() == expect_ids


@pytest.mark.parametrize(
    "regex, regex_exclude, query, expect_ids",
    [
        ("exp[23]", None, "`config/foo1-unique-id-exp-2`:int = 1", ["id-exp-2"]),
        ("exp[23]", None, "`config/bar1`:int = 1", ["id-exp-2", "id-exp-3"]),
        ("exp", "exp1", "`config/foo1-unique-id-exp-2`:int = 1", ["id-exp-2"]),
        (None, "exp[^2]", "`config/bar1`:int = 1", ["id-exp-2"]),
        (
            "exp[234]",
            "exp4",
            "`config/bar1`:int = 1 OR `config/foo1-unique-id-exp-2`:int = 1",
            ["id-exp-2", "id-exp-3"],
        ),
        (
            "exp",
            None,
            "`config/foo1-unique-id-exp-2`:int = 1 OR `config/foo1-unique-id-exp-3`:int = 1",
            ["id-exp-2", "id-exp-3"],
        ),
    ],
)
def test__experiments_nql_query_with_names_regex(project, regex, regex_exclude, query, expect_ids):
    df = project.fetch_experiments_df(
        names_regex=regex, names_exclude_regex=regex_exclude, query=query, sort_by="sys/custom_run_id", ascending=True
    )

    assert df["sys/custom_run_id"].tolist() == expect_ids


@pytest.mark.parametrize(
    "regex, query, expect_ids",
    [
        ("id-run-[23]", "`config/foo1-unique-id-run-2`:int = 1", ["id-run-2"]),
        (None, "`config/foo1-unique-id-run-2`:int = 1", ["id-run-2"]),
        ("id-run-[23]", "`config/bar1`:int = 1", ["id-run-2", "id-run-3"]),
        (
            "id-run",
            "`config/foo1-unique-id-run-2`:int = 1 OR `config/foo1-unique-id-run-3`:int = 1",
            ["id-run-2", "id-run-3"],
        ),
        (
            None,
            "`config/foo1-unique-id-run-2`:int = 1 OR `config/foo1-unique-id-run-3`:int = 1",
            ["id-run-2", "id-run-3"],
        ),
    ],
)
def test__runs_nql_query_with_custom_id_regex(project, regex, query, expect_ids):
    # Query narrows down a broader regex, so only one run  is returned
    df = project.fetch_runs_df(
        custom_id_regex=regex,
        query=query,
        sort_by="sys/custom_run_id",
        ascending=True,
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids


def test__runs_nql_query_with_tags(project):
    df = project.fetch_runs_df(
        tags=["head"],
        query="(`config/foo1-unique-id-run-1`:int = 1)",
        sort_by="sys/custom_run_id",
        ascending=True,
    )

    assert df["sys/custom_run_id"].tolist() == ["id-run-1"]


def test__runs_nql_query_with_custom_ids(project):
    df = project.fetch_runs_df(
        sort_by="sys/custom_run_id",
        ascending=True,
        custom_ids=["id-run-1", "id-run-2"],
        query="(`config/foo1-unique-id-run-1`:int = 1)",
    )

    assert df["sys/custom_run_id"].tolist() == ["id-run-1"]


def test__experiments_nql_query_with_tags(project):
    df = project.fetch_experiments_df(
        tags=["head"],
        query="(`config/foo1-unique-id-exp-1`:int = 1)",
        sort_by="sys/custom_run_id",
        ascending=True,
    )

    assert df["sys/custom_run_id"].tolist() == ["id-exp-1"]


def test__experiments_nql_query_with_custom_ids(project):
    df = project.fetch_experiments_df(
        sort_by="sys/custom_run_id",
        ascending=True,
        custom_ids=["id-exp-1", "id-exp-2"],
        query="(`config/foo1-unique-id-exp-1`:int = 1)",
    )

    assert df["sys/custom_run_id"].tolist() == ["id-exp-1"]


@pytest.mark.parametrize(
    "regex, regex_neg, expect_ids",
    [
        ("exp.*", None, ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"]),
        ("exp.*", "exp[23]", ["id-exp-1", "id-exp-4", "id-exp-5", "id-exp-6"]),
        (None, "exp[23]", ["id-exp-1", "id-exp-4", "id-exp-5", "id-exp-6"]),
        ("exp[234]", "exp[4]", ["id-exp-2", "id-exp-3"]),
        (None, "exp", []),
        (["exp", "1"], None, ["id-exp-1"]),
        (["e", "x", "p"], None, ["id-exp-1", "id-exp-2", "id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"]),
        (["exp1", "exp2"], None, []),
        (None, ["exp1", "exp2"], ["id-exp-3", "id-exp-4", "id-exp-5", "id-exp-6"]),
        (None, ["foo", "exp"], []),
    ],
)
def test__experiments_by_name_regex_and_regex_neg(project, sys_columns, regex, regex_neg, expect_ids):
    df = project.fetch_experiments_df(
        columns=sys_columns,
        names_regex=regex,
        names_exclude_regex=regex_neg,
        sort_by="sys/custom_run_id",
        ascending=True,
    )
    assert df["sys/custom_run_id"].tolist() == expect_ids
