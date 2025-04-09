import pytest

import neptune_fetcher.runs as runs
from neptune_fetcher import Context
from neptune_fetcher.filters import (
    Attribute,
    Filter,
)
from tests.e2e.generator import (
    ALL_STATIC_RUNS,
    LINEAR_HISTORY_TREE,
)


@pytest.mark.parametrize(
    "all_filter",
    [
        ".*",
        None,
        [run.custom_run_id for run in ALL_STATIC_RUNS],
        Filter.name_in(*[run.experiment_name for run in ALL_STATIC_RUNS]),
    ],
)
def test_list_all_runs(new_project_context: Context, all_filter):
    result = runs.list_runs(all_filter, context=new_project_context)
    assert len(result) == len(ALL_STATIC_RUNS)
    assert set(result) == {run.custom_run_id for run in ALL_STATIC_RUNS}


@pytest.mark.parametrize(
    "linear_history_filter",
    [
        "linear.*",
        [run.custom_run_id for run in LINEAR_HISTORY_TREE],
        Filter.name_in(*[run.experiment_name for run in LINEAR_HISTORY_TREE]),
        Filter.eq("linear-history", True),
        Filter.eq(Attribute(name="linear-history", type="bool"), True),
        Filter.eq(Attribute(name="linear-history"), True),
        # TODO string set filter
        # Filter.eq(Attribute(name="sys/tags", type="string_set"), ["linear"]),
    ],
)
def test_list_linear_history_runs(new_project_context: Context, linear_history_filter):
    result = runs.list_runs(linear_history_filter, context=new_project_context)
    assert len(result) == len(LINEAR_HISTORY_TREE)
    assert set(result) == {run.custom_run_id for run in LINEAR_HISTORY_TREE}


@pytest.mark.parametrize(
    "linear_history_filter",
    [
        "abc",
        ["abc"],
        Filter.eq(Attribute(name="non-existent", type="bool"), True),
    ],
)
def test_list_runs_empty_filter(new_project_context: Context, linear_history_filter):
    result = runs.list_runs(linear_history_filter, context=new_project_context)

    assert set(result) == set()
