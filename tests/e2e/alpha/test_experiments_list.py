import pytest

import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.filters import (
    Attribute,
    Filter,
)
from tests.e2e.alpha.generator import (
    ALL_STATIC_EXPERIMENTS,
    EXPERIMENTS_BY_NAME,
    FORKED_TREE_EXP_NAME,
    LINEAR_HISTORY_TREE,
    LINEAR_TREE_EXP_NAME,
)


@pytest.mark.parametrize(
    "all_filter",
    [".*", None, Filter.name_in(*[run.experiment_name for run in ALL_STATIC_EXPERIMENTS])],
)
def test_list_all_runs(new_project_context: Context, all_filter):
    result = npt.list_experiments(all_filter, context=new_project_context)
    assert len(result) == len(ALL_STATIC_EXPERIMENTS)
    assert set(result) == set(EXPERIMENTS_BY_NAME.keys())


@pytest.mark.parametrize(
    ("linear_history_filter", "expected"),
    [
        ("linear.*", [LINEAR_TREE_EXP_NAME]),
        (Filter.name_in(*[run.experiment_name for run in LINEAR_HISTORY_TREE]), [LINEAR_TREE_EXP_NAME]),
        (Filter.eq("linear-history", True), [LINEAR_TREE_EXP_NAME]),
        (Filter.eq(Attribute(name="linear-history", type="bool"), True), [LINEAR_TREE_EXP_NAME]),
        (
            Filter.eq(Attribute(name="foo0", type="float_series"), 0.7 * 19),
            [LINEAR_TREE_EXP_NAME, FORKED_TREE_EXP_NAME],
        ),
        # TODO string set filter
        # Filter.eq(Attribute(name="sys/tags", type="string_set"), ["linear"]),
    ],
)
def test_list_linear_history_runs(new_project_context: Context, linear_history_filter, expected):
    result = npt.list_experiments(linear_history_filter, context=new_project_context)
    assert len(result) == len(expected)
    assert set(result) == set(expected)


@pytest.mark.parametrize(
    "linear_history_filter",
    [
        "abc",
        Filter.eq(Attribute(name="non-existent", type="bool"), True),
    ],
)
def test_list_runs_empty_filter(new_project_context: Context, linear_history_filter):
    result = npt.list_experiments(linear_history_filter, context=new_project_context)

    assert result == []
