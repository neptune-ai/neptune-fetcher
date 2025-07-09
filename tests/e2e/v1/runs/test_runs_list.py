import os

import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    Filter,
)
from tests.e2e.v1.generator import (
    ALL_STATIC_RUNS,
    LINEAR_HISTORY_TREE,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize(
    "arg_runs",
    [
        ".*",
        None,
        [run.custom_run_id for run in ALL_STATIC_RUNS],
        Filter.name([run.experiment_name for run in ALL_STATIC_RUNS]),
        Filter.name(" | ".join(run.experiment_name for run in ALL_STATIC_RUNS)),
    ],
)
def test_list_all_runs(new_project_id, arg_runs):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
    )
    assert len(result) == len(ALL_STATIC_RUNS)
    assert set(result) == {run.custom_run_id for run in ALL_STATIC_RUNS}


@pytest.mark.parametrize(
    "arg_runs",
    [
        "linear.*",
        [run.custom_run_id for run in LINEAR_HISTORY_TREE],
        Filter.name([run.experiment_name for run in LINEAR_HISTORY_TREE]),
        Filter.name(" | ".join(run.experiment_name for run in LINEAR_HISTORY_TREE)),
        Filter.eq("linear-history", True),
        Filter.eq(Attribute(name="linear-history", type="bool"), True),
        Filter.eq(Attribute(name="linear-history"), True),
        # TODO string set filter
        # Filter.eq(Attribute(name="sys/tags", type="string_set"), ["linear"]),
    ],
)
def test_list_linear_history_runs(new_project_id, arg_runs):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
    )
    assert len(result) == len(LINEAR_HISTORY_TREE)
    assert set(result) == {run.custom_run_id for run in LINEAR_HISTORY_TREE}


@pytest.mark.parametrize(
    "arg_runs",
    [
        "abc",
        ["abc"],
        Filter.eq(Attribute(name="non-existent", type="bool"), True),
    ],
)
def test_list_runs_empty_filter(new_project_id, arg_runs):
    result = runs.list_runs(
        project=new_project_id,
        runs=arg_runs,
    )

    assert set(result) == set()
