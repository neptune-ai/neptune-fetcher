import pytest

import neptune_fetcher.alpha.runs as runs
from neptune_fetcher.alpha import Context
from neptune_fetcher.internal.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.alpha.generator import (
    ALL_STATIC_RUNS,
    LINEAR_HISTORY_TREE,
)


@pytest.mark.parametrize(
    "filter_, expected",
    [
        (".*", ALL_STATIC_RUNS),
        (None, ALL_STATIC_RUNS),
        (Filter.name_in(*[run.experiment_name for run in ALL_STATIC_RUNS]), ALL_STATIC_RUNS),
        ([run.custom_run_id for run in ALL_STATIC_RUNS], ALL_STATIC_RUNS),
        ("linear.*", LINEAR_HISTORY_TREE),
        (Filter.name_in(*[run.experiment_name for run in LINEAR_HISTORY_TREE]), LINEAR_HISTORY_TREE),
        (Filter.eq("linear-history", True), LINEAR_HISTORY_TREE),
        (Filter.eq(Attribute(name="linear-history", type="bool"), True), LINEAR_HISTORY_TREE),
    ],
)
def test_list_attributes(new_project_context: Context, filter_, expected):
    attributes = runs.list_attributes(filter_, None, context=new_project_context)
    expected = set.union(*[r.attributes() for r in expected])

    assert _filter_out_sys(attributes) == expected


@pytest.mark.parametrize(
    "_attr_filter, expected",
    [
        # DateTime attributes
        (AttributeFilter(name_eq="datetime-value", type_in=["datetime"]), {"datetime-value"}),
        # Numeric series
        (AttributeFilter(name_eq="unique1/0", type_in=["float_series"]), {"unique1/0"}),
        (AttributeFilter(name_eq="foo0", type_in=["float_series"]), {"foo0"}),
        (AttributeFilter(name_eq="foo1", type_in=["float_series"]), {"foo1"}),
        # Primitive types
        (AttributeFilter(type_in=["int"]), {"int-value"}),
        (AttributeFilter(type_in=["float"]), {"float-value"}),
        (AttributeFilter(type_in=["string"]), {"str-value"}),
        (AttributeFilter(type_in=["bool"]), {"bool-value"}),
        # Multiple types
        (AttributeFilter(type_in=["float", "int"]), {"float-value", "int-value"}),
        # Name patterns
        (AttributeFilter(name_matches_all="unique.*"), {"unique1/0", "unique2/0"}),
        (AttributeFilter(name_matches_all="foo.*"), {"foo0", "foo1"}),
        # Combined filters
        (AttributeFilter(name_matches_all=".*value.*", type_in=["float"]), {"float-value"}),
        (AttributeFilter(name_matches_all=".*value.*", type_in=["int"]), {"int-value"}),
        (
            AttributeFilter(name_matches_all=".*value.*", type_in=["float"])
            | AttributeFilter(name_matches_all=".*value.*", type_in=["int"]),
            {"float-value", "int-value"},
        ),
    ],
)
def test_list_attributes_with_attribute_filter(new_project_context: Context, _attr_filter, expected):
    attributes = runs.list_attributes(
        "^forked_history_root$|^forked_history_fork1$", _attr_filter, context=new_project_context
    )

    assert _filter_out_sys(attributes) == expected


def _filter_out_sys(attributes):
    return {attr for attr in attributes if not attr.startswith("sys/")}
