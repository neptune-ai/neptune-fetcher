import os
from typing import Iterable

import pytest

from neptune_fetcher.alpha import list_attributes
from neptune_fetcher.internal.filters import (
    _Attribute,
    _AttributeFilter,
    _Filter,
)
from tests.e2e.alpha.internal.data import (
    FLOAT_SERIES_PATHS,
    PATH,
    STRING_SERIES_PATHS,
    TEST_DATA,
    TEST_DATA_VERSION,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def _drop_sys_attr_names(attributes: Iterable[str]) -> list[str]:
    return [attr for attr in attributes if not attr.startswith("sys/")]


# Convenience filter to limit searches to experiments belonging to this test,
# in case the run has some extra experiments.
EXPERIMENTS_IN_THIS_TEST = _Filter.name_in(*TEST_DATA.experiment_names)


@pytest.mark.parametrize(
    "experiment_filter",
    (
        EXPERIMENTS_IN_THIS_TEST,
        TEST_DATA.experiment_names,
        rf"test_alpha_[0-9]+_{TEST_DATA_VERSION}",
        ".*",
        None,
    ),
)
@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (PATH, TEST_DATA.all_attribute_names),
        (f"{PATH}/int-value", {f"{PATH}/int-value"}),
        (rf"{PATH}/metrics/.*", FLOAT_SERIES_PATHS + [f"{PATH}/metrics/step"] + STRING_SERIES_PATHS),
        (
            rf"{PATH}/files/.*",
            {f"{PATH}/files/file-value", f"{PATH}/files/file-value.txt", f"{PATH}/files/object-does-not-exist"},
        ),
        (
            rf"{PATH}/.*-value$",
            {
                f"{PATH}/int-value",
                f"{PATH}/float-value",
                f"{PATH}/str-value",
                f"{PATH}/bool-value",
                f"{PATH}/datetime-value",
                f"{PATH}/string_set-value",
                f"{PATH}/files/file-value",
            },
        ),
        (rf"{PATH}/unique-value-[0-9]", {f"{PATH}/unique-value-{i}" for i in range(6)}),
        (_AttributeFilter(name_matches_all=PATH), TEST_DATA.all_attribute_names),
        (_AttributeFilter(name_eq=f"{PATH}/float-value"), {f"{PATH}/float-value"}),
        (
            _AttributeFilter.any(_AttributeFilter(name_matches_all="^(foo)"), _AttributeFilter(name_matches_all=PATH)),
            TEST_DATA.all_attribute_names,
        ),
        (_AttributeFilter(name_matches_none=".*"), []),
    ],
)
def test_list_attributes_known_in_all_experiments_with_name_filter_excluding_sys(
    attribute_filter, expected, experiment_filter
):
    attributes = _drop_sys_attr_names(list_attributes(attributes=attribute_filter, experiments=experiment_filter))
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "name_filter",
    (
        None,
        "",
        ".*",
        _AttributeFilter(name_matches_all=".*"),
        _AttributeFilter(),
    ),
)
def test_list_attributes_all_names_from_all_experiments_excluding_sys(name_filter):
    attributes = _drop_sys_attr_names(list_attributes(experiments=EXPERIMENTS_IN_THIS_TEST, attributes=name_filter))
    assert set(attributes) == set(TEST_DATA.all_attribute_names)
    assert len(attributes) == len(TEST_DATA.all_attribute_names)


@pytest.mark.parametrize(
    "filter_",
    (
        "unknown",
        ".*unknown.*",
        "sys/abcdef",
        " ",
        _AttributeFilter(name_eq=".*"),
        _AttributeFilter(name_matches_all="unknown"),
    ),
)
def test_list_attributes_unknown_name(filter_):
    attributes = list_attributes(attributes=filter_)
    assert not attributes


@pytest.mark.parametrize(
    "attribute_filter, experiment_filter, expected",
    [
        (r"unique-value-[0-2]", EXPERIMENTS_IN_THIS_TEST, {f"{PATH}/unique-value-{i}" for i in range(3)}),
        (
            rf"{PATH}/unique-value-[0-2]",
            f"test_alpha_.*_{TEST_DATA_VERSION}",
            {f"{PATH}/unique-value-{i}" for i in range(3)},
        ),
        (
            rf"{PATH}/unique-value-.*",
            rf"test_alpha_(0|2)_{TEST_DATA_VERSION}",
            {f"{PATH}/unique-value-0", f"{PATH}/unique-value-2"},
        ),
        (
            rf"{PATH}/unique-value-.*",
            _Filter.contains_all(_Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0"),
            {f"{PATH}/unique-value-0"},
        ),
        (
            rf"{PATH}/unique-value-.*",
            _Filter.contains_none(
                _Attribute(f"{PATH}/string_set-value", type="string_set"), ["string-0-0", "string-1-0", "string-4-0"]
            ),
            {f"{PATH}/unique-value-{i}" for i in (2, 3, 5)},
        ),
        (
            [f"{PATH}/int-value", f"{PATH}/float-value"],
            TEST_DATA.experiment_names,
            {f"{PATH}/int-value", f"{PATH}/float-value"},
        ),
        (
            _AttributeFilter(name_matches_none="sys/.*", name_matches_all=".*"),
            _Filter.gt(_Attribute(f"{PATH}/int-value", type="int"), 1234) & EXPERIMENTS_IN_THIS_TEST,
            [],
        ),
        (
            _AttributeFilter(name_matches_none="sys/.*", name_matches_all=".*"),
            _Filter.eq(_Attribute(f"{PATH}/str-value", type="string"), "hello_12345") & EXPERIMENTS_IN_THIS_TEST,
            [],
        ),
        (
            f"{PATH}/unique-value",
            _Filter.lt(_Attribute(f"{PATH}/int-value", type="int"), 3) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in range(3)},
        ),
        (
            f"{PATH}/unique-value",
            _Filter.eq(_Attribute(f"{PATH}/bool-value", type="bool"), False) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in (1, 3, 5)},
        ),
        (
            f"{PATH}/unique-value",
            _Filter.eq(_Attribute(f"{PATH}/bool-value", type="bool"), False) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in (1, 3, 5)},
        ),
    ],
)
def test_list_attributes_depending_on_values_in_experiments(attribute_filter, experiment_filter, expected):
    attributes = list_attributes(attributes=attribute_filter, experiments=experiment_filter)
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (
            r"sys/(name|id)",
            {"sys/name", "sys/id"},
        ),
        (r"sys/.*id$", {"sys/custom_run_id", "sys/id", "sys/diagnostics/project_uuid", "sys/diagnostics/run_uuid"}),
        (_AttributeFilter(name_matches_all=r"sys/(name|id)"), {"sys/name", "sys/id"}),
    ],
)
def test_list_attributes_sys_attrs(attribute_filter, expected):
    """A separate test for sys attributes, as we ignore them in tests above for simplicity."""

    attributes = list_attributes(attributes=attribute_filter)
    assert set(attributes) == expected
    assert len(attributes) == len(expected)
