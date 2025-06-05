import os

import pytest

import neptune_fetcher.v1.runs as runs
from neptune_fetcher.v1.filters import (
    Attribute,
    Filter,
)
from tests.e2e.alpha.generator import (
    ALL_STATIC_RUNS,
    LINEAR_HISTORY_TREE,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize(
    "arg_runs, arg_where",
    [
        (".*", None),
        (None, None),
        ([run.custom_run_id for run in ALL_STATIC_RUNS], None),
        (None, Filter.name_in(*[run.experiment_name for run in ALL_STATIC_RUNS])),
    ],
)
def test_list_all_runs(new_project_id, arg_runs, arg_where):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
        where=arg_where,
    )
    assert len(result) == len(ALL_STATIC_RUNS)
    assert set(result) == {run.custom_run_id for run in ALL_STATIC_RUNS}


@pytest.mark.parametrize(
    "arg_runs, arg_where",
    [
        ("linear.*", None),
        ([run.custom_run_id for run in LINEAR_HISTORY_TREE], None),
        (None, Filter.name_in(*[run.experiment_name for run in LINEAR_HISTORY_TREE])),
        (None, Filter.eq("linear-history", True)),
        (None, Filter.eq(Attribute(name="linear-history", type="bool"), True)),
        (None, Filter.eq(Attribute(name="linear-history"), True)),
        # TODO string set filter
        # (None, Filter.eq(Attribute(name="sys/tags", type="string_set"), ["linear"])),
    ],
)
def test_list_linear_history_runs(new_project_id, arg_runs, arg_where):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
        where=arg_where,
    )
    assert len(result) == len(LINEAR_HISTORY_TREE)
    assert set(result) == {run.custom_run_id for run in LINEAR_HISTORY_TREE}


@pytest.mark.parametrize(
    "arg_runs, arg_where",
    [
        ("abc", None),
        (["abc"], None),
        (None, Filter.eq(Attribute(name="non-existent", type="bool"), True)),
    ],
)
def test_list_runs_empty_filter(new_project_id, arg_runs, arg_where):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
        where=arg_where,
    )

    assert set(result) == set()
