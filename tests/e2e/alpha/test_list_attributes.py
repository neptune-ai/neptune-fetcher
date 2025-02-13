import pytest

import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
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
    "filter_, expected",
    [
        (".*", ALL_STATIC_EXPERIMENTS),
        (None, ALL_STATIC_EXPERIMENTS),
        (Filter.eq(Attribute(name="sys/custom_run_id", type="string"), "non-existent"), []),
        (Filter.name_in(*[run.experiment_name for run in ALL_STATIC_EXPERIMENTS]), ALL_STATIC_EXPERIMENTS),
        ("linear.*", [EXPERIMENTS_BY_NAME[LINEAR_TREE_EXP_NAME]]),
        (
            Filter.name_in(*[run.experiment_name for run in LINEAR_HISTORY_TREE]),
            [EXPERIMENTS_BY_NAME[LINEAR_TREE_EXP_NAME]],
        ),
        (Filter.name_in(LINEAR_TREE_EXP_NAME), [EXPERIMENTS_BY_NAME[LINEAR_TREE_EXP_NAME]]),
        (Filter.eq("linear-history", True), [EXPERIMENTS_BY_NAME[LINEAR_TREE_EXP_NAME]]),
        (Filter.eq(Attribute(name="linear-history", type="bool"), True), [EXPERIMENTS_BY_NAME[LINEAR_TREE_EXP_NAME]]),
    ],
)
def test_list_attributes(new_project_context: Context, filter_, expected):
    attributes = npt.list_attributes(filter_, None, context=new_project_context)
    expected = set().union(*[r.attributes() for r in expected])

    assert _filter_out_sys(attributes) == expected


@pytest.mark.parametrize(
    "_attr_filter, expected",
    [
        # Non-existent attribute
        (AttributeFilter(name_eq="non-existent", type_in=["int"]), set()),
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
        (AttributeFilter(name_matches_all="unique.*"), {"unique1/0", "unique3/0"}),
        (AttributeFilter(name_matches_all="foo.*"), {"foo0", "foo1"}),
        # Combined filters
        (AttributeFilter(name_matches_all=".*value.*", type_in=["float"]), {"float-value"}),
        (AttributeFilter(name_matches_all=".*value.*", type_in=["int"]), {"int-value"}),
        (
            AttributeFilter(name_matches_all=".*value.*", type_in=["float"])
            | AttributeFilter(name_matches_all=".*value.*", type_in=["int"]),
            {"float-value", "int-value"},
        ),
        (AttributeFilter(name_matches_none=".*value.*"), {'foo1', 'foo0', 'unique3/0', 'unique1/0'}),
        (AttributeFilter(name_matches_all=".*value.*", name_matches_none=".*value.*"), set()),
    ],
)
def test_list_attributes_with_attribute_filter(new_project_context: Context, _attr_filter, expected):
    attributes = npt.list_attributes(f"^{FORKED_TREE_EXP_NAME}$", _attr_filter, context=new_project_context)

    assert _filter_out_sys(attributes) == expected


@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (
            r"sys/(name|id)",
            {"sys/name", "sys/id"},
        ),
        (r"sys/.*id$", {"sys/custom_run_id", "sys/id"}),
        (AttributeFilter(name_matches_all=r"sys/(name|id)"), {"sys/name", "sys/id"}),
    ],
)
def test_list_attributes_sys_attrs(new_project_context: Context, attribute_filter, expected):
    """A separate test for sys attributes, as we ignore them in tests above for simplicity."""

    attributes = npt.list_attributes(attributes=attribute_filter, context=new_project_context)
    assert set(attributes) == expected
    assert len(attributes) == len(expected)


def _filter_out_sys(attributes):
    return {attr for attr in attributes if not attr.startswith("sys/")}
