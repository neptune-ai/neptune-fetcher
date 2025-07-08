import os

import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.v1.generator import (
    ALL_STATIC_RUNS,
    LINEAR_HISTORY_TREE,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize(
    "arg_runs, expected",
    [
        (".*", ALL_STATIC_RUNS),
        (None, ALL_STATIC_RUNS),
        (Filter.name([run.experiment_name for run in ALL_STATIC_RUNS]), ALL_STATIC_RUNS),
        ([run.custom_run_id for run in ALL_STATIC_RUNS], ALL_STATIC_RUNS),
        ("linear.*", LINEAR_HISTORY_TREE),
        (Filter.name([run.experiment_name for run in LINEAR_HISTORY_TREE]), LINEAR_HISTORY_TREE),
        (Filter.eq("linear-history", True), LINEAR_HISTORY_TREE),
        (Filter.eq(Attribute(name="linear-history", type="bool"), True), LINEAR_HISTORY_TREE),
    ],
)
def test_list_attributes(new_project_id, arg_runs, expected):
    attributes = runs.list_attributes(
        project=new_project_id,
        runs=arg_runs,
        attributes=None,
    )
    expected = set.union(*[r.attributes() for r in expected])

    assert _filter_out_sys(attributes) == expected


@pytest.mark.parametrize(
    "_attr_filter, expected",
    [
        # DateTime attributes
        (AttributeFilter(name="datetime-value", type=["datetime"]), {"datetime-value"}),
        # Numeric series
        (AttributeFilter(name="unique1/0", type=["float_series"]), {"unique1/0"}),
        (AttributeFilter(name="foo0", type=["float_series"]), {"foo0"}),
        (AttributeFilter(name="foo1", type=["float_series"]), {"foo1"}),
        # Primitive types
        (AttributeFilter(type=["int"]), {"int-value"}),
        (AttributeFilter(type=["float"]), {"float-value"}),
        (AttributeFilter(type=["string"]), {"str-value"}),
        (AttributeFilter(type=["bool"]), {"bool-value"}),
        # Multiple types
        (AttributeFilter(type=["float", "int"]), {"float-value", "int-value"}),
        # Name patterns
        (AttributeFilter(name="unique.*"), {"unique1/0", "unique2/0"}),
        (AttributeFilter(name="foo.*"), {"foo0", "foo1"}),
        # Combined filters
        (AttributeFilter(name=".*value.*", type=["float"]), {"float-value"}),
        (AttributeFilter(name=".*value.*", type=["int"]), {"int-value"}),
        (
            AttributeFilter(name=".*value.*", type=["float"]) | AttributeFilter(name=".*value.*", type=["int"]),
            {"float-value", "int-value"},
        ),
    ],
)
def test_list_attributes_with_attribute_filter(new_project_id, _attr_filter, expected):
    attributes = runs.list_attributes(
        project=new_project_id,
        runs="^forked_history_root$|^forked_history_fork1$",
        attributes=_attr_filter,
    )

    assert _filter_out_sys(attributes) == expected


def _filter_out_sys(attributes):
    return {attr for attr in attributes if not attr.startswith("sys/")}
