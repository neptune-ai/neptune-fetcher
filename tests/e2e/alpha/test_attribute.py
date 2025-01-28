import itertools as it
import os
import re
import time
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_fetcher.alpha.filter import AttributeFilter
from neptune_fetcher.alpha.internal.attribute import (
    AttributeDefinition,
    fetch_attribute_definitions,
    fetch_attribute_values,
)
from neptune_fetcher.alpha.internal.identifiers import ExperimentIdentifier
from neptune_fetcher.alpha.internal.types import (
    AttributeValue,
    FloatSeriesAggregatesSubset,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = "pye2e-fetcher-test-attribute"
COMMON_PATH = "test_attribute"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(project):
    import uuid

    from neptune_scale import Run

    project_identifier = project.project_identifier
    run_id = str(uuid.uuid4())

    run = Run(
        project=project_identifier,
        run_id=run_id,
        experiment_name=EXPERIMENT_NAME,
    )

    data = {
        f"{COMMON_PATH}/int-value": 10,
        f"{COMMON_PATH}/float-value": 0.5,
        f"{COMMON_PATH}/str-value": "hello",
        f"{COMMON_PATH}/bool-value": True,
        f"{COMMON_PATH}/datetime-value": DATETIME_VALUE,
    }
    run.log_configs(data)

    path = f"{COMMON_PATH}/float-series-value"
    for step, value in zip(FLOAT_SERIES_STEPS, FLOAT_SERIES_VALUES):
        run.log_metrics(data={path: value}, step=step)

    run.add_tags({"string-set-item"})  # the only way to write string-set type. It's implicit path is sys/tags

    now = time.time()
    data = {
        f"{COMMON_PATH}/int_value_a": int(now),
        f"{COMMON_PATH}/int_value_b": int(now),
        f"{COMMON_PATH}/float_value_a": now,
        f"{COMMON_PATH}/float_value_b": now,
    }
    run.log_configs(data)

    run.wait_for_processing()

    return run


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> ExperimentIdentifier:
    from neptune_fetcher.alpha.filter import ExperimentFilter
    from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = ExperimentFilter.name_in(EXPERIMENT_NAME)
    experiment_attrs = _extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, experiment_filter=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return ExperimentIdentifier(project_identifier, sys_id)


def test_fetch_attribute_definitions_single_string(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string", attribute_filter)]


def test_fetch_attribute_definitions_does_not_exist(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="does-not-exist", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert attributes == []


def test_fetch_attribute_definitions_two_strings(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq=["sys/name", "sys/owner"], type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/name", "string", attribute_filter),
            AttributeDefinition("sys/owner", "string", attribute_filter),
        ],
    )


def test_fetch_attribute_definitions_single_series(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    path = f"{COMMON_PATH}/float-series-value"

    #  when
    attribute_filter = AttributeFilter(name_eq=path, type_in=["float_series"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert attributes == [AttributeDefinition(path, "float_series", attribute_filter)]


def test_fetch_attribute_definitions_all_types(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    all_attrs = [
        (f"{COMMON_PATH}/int-value", "int"),
        (f"{COMMON_PATH}/float-value", "float"),
        (f"{COMMON_PATH}/str-value", "string"),
        (f"{COMMON_PATH}/bool-value", "bool"),
        (f"{COMMON_PATH}/datetime-value", "datetime"),
        (f"{COMMON_PATH}/float-series-value", "float_series"),
        ("sys/tags", "string_set"),
    ]

    #  when
    attribute_filter = AttributeFilter(name_eq=[name for name, _ in all_attrs])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    expected_definitions = [AttributeDefinition(name, type, attribute_filter) for name, type in all_attrs]
    assert assert_items_equal(attributes, expected_definitions)


def test_fetch_attribute_definitions_no_type_in(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name")
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string", attribute_filter)]


def test_fetch_attribute_definitions_regex_matches_all(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime", attribute_filter),
            AttributeDefinition("sys/modification_time", "datetime", attribute_filter),
            AttributeDefinition("sys/ping_time", "datetime", attribute_filter),
        ],
    )


def test_fetch_attribute_definitions_regex_matches_none(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(
        name_matches_all="sys/.*_time", name_matches_none="modification", type_in=["datetime"]
    )
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime", attribute_filter),
            AttributeDefinition("sys/ping_time", "datetime", attribute_filter),
        ],
    )


def test_fetch_attribute_definitions_multiple_projects(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    project_identifier_2 = f"{project_identifier}-does-not-exist"

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier, project_identifier, project_identifier_2],
            [experiment_identifier],
            attribute_filter=attribute_filter,
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string", attribute_filter)]


def test_fetch_attribute_definitions_filter_or(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["float"])

    #  when
    attribute_filter = attribute_filter_1 | attribute_filter_2
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int", attribute_filter_1),
            AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float", attribute_filter_2),
        ],
    )


@pytest.mark.parametrize(
    "make_attribute_filter",
    [
        lambda a, b, c: a | b | c,
        lambda a, b, c: AttributeFilter.any(a, b, c),
        lambda a, b, c: AttributeFilter.any(a, AttributeFilter.any(b, c)),
    ],
)
def test_fetch_attribute_definitions_filter_triple_or(client, project, experiment_identifier, make_attribute_filter):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["float"])
    attribute_filter_3 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["int"])
    attribute_filter = make_attribute_filter(attribute_filter_1, attribute_filter_2, attribute_filter_3)

    #  when
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
        )
    )

    # then
    assert assert_items_equal(
        attributes,
        [
            AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int", attribute_filter_1),
            AttributeDefinition(f"{COMMON_PATH}/int_value_b", "int", attribute_filter_3),
            AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float", attribute_filter_2),
        ],
    )


def test_fetch_attribute_definitions_paging(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter, batch_size=1
        )
    )

    # then
    assert assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime", attribute_filter),
            AttributeDefinition("sys/modification_time", "datetime", attribute_filter),
            AttributeDefinition("sys/ping_time", "datetime", attribute_filter),
        ],
    )


def test_fetch_attribute_definitions_paging_executor(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])

    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter, batch_size=1
        )
    )

    # then
    assert assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime", attribute_filter),
            AttributeDefinition("sys/modification_time", "datetime", attribute_filter),
            AttributeDefinition("sys/ping_time", "datetime", attribute_filter),
        ],
    )


def test_fetch_attribute_definitions_should_deduplicate_items(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    for i in range(10):
        attribute_filter = attribute_filter | AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])

    attributes = _extract_pages(
        fetch_attribute_definitions(
            client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter, batch_size=1
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime", attribute_filter),
            AttributeDefinition("sys/modification_time", "datetime", attribute_filter),
            AttributeDefinition("sys/ping_time", "datetime", attribute_filter),
        ],
    )


def test_fetch_attribute_values_single_string(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition("sys/name", "string", AttributeFilter(name_eq="sys/name"))],
        )
    )

    # then
    assert values == [AttributeValue("sys/name", "string", EXPERIMENT_NAME)]


def test_fetch_attribute_values_two_strings(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition("sys/name", "string", AttributeFilter(name_eq="sys/name")),
                AttributeDefinition("sys/owner", "string", AttributeFilter(name_eq="sys/owner")),
            ],
        )
    )

    # then
    assert set((value.name, value.type) for value in values) == {
        ("sys/name", "string"),
        ("sys/owner", "string"),
    }


def test_fetch_attribute_values_single_series_all_aggregations(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    path = f"{COMMON_PATH}/float-series-value"

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition(
                    path,
                    "float_series",
                    AttributeFilter(name_eq=path, aggregations=["last", "min", "max", "average", "variance"]),
                )
            ],
        )
    )

    # then
    average = sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES)
    aggregates = FloatSeriesAggregatesSubset(
        last=FLOAT_SERIES_VALUES[-1],
        min=min(FLOAT_SERIES_VALUES),
        max=max(FLOAT_SERIES_VALUES),
        average=average,
        variance=sum((value - average) ** 2 for value in FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
    )
    assert values == [AttributeValue(path, "float_series", aggregates)]


def test_fetch_attribute_values_single_series_default_aggregations(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    path = f"{COMMON_PATH}/float-series-value"

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(path, "float_series", AttributeFilter(name_eq=path))],
        )
    )

    # then
    aggregates = FloatSeriesAggregatesSubset(
        last=FLOAT_SERIES_VALUES[-1],
    )
    assert values == [AttributeValue(path, "float_series", aggregates)]


def test_fetch_attribute_values_single_series_selected_aggregations(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    path = f"{COMMON_PATH}/float-series-value"

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(path, "float_series", AttributeFilter(name_eq=path, aggregations=["min", "max"]))],
        )
    )

    # then
    aggregates = FloatSeriesAggregatesSubset(
        min=min(FLOAT_SERIES_VALUES),
        max=max(FLOAT_SERIES_VALUES),
    )
    assert values == [AttributeValue(path, "float_series", aggregates)]


def test_fetch_attribute_values_all_types(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    all_values = [
        AttributeValue(f"{COMMON_PATH}/int-value", "int", 10),
        AttributeValue(f"{COMMON_PATH}/float-value", "float", 0.5),
        AttributeValue(f"{COMMON_PATH}/str-value", "string", "hello"),
        AttributeValue(f"{COMMON_PATH}/bool-value", "bool", True),
        AttributeValue(f"{COMMON_PATH}/datetime-value", "datetime", DATETIME_VALUE),
        AttributeValue(
            f"{COMMON_PATH}/float-series-value",
            "float_series",
            FloatSeriesAggregatesSubset(
                last=FLOAT_SERIES_VALUES[-1],
            ),
        ),
        AttributeValue("sys/tags", "string_set", {"string-set-item"}),
    ]

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(attr.name, attr.type, AttributeFilter(name_eq=attr.name)) for attr in all_values],
        )
    )

    # then
    assert len(values) == len(all_values)
    for expected in all_values:
        value = next(value for value in values if value.name == expected.name)
        assert value == expected


def test_fetch_attribute_values_paging(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition("sys/creation_time", "datetime", AttributeFilter(name_eq="sys/creation_time")),
                AttributeDefinition(
                    "sys/modification_time", "datetime", AttributeFilter(name_eq="sys/modification_time")
                ),
                AttributeDefinition("sys/ping_time", "datetime", AttributeFilter(name_eq="sys/ping_time")),
            ],
            batch_size=1,
        )
    )

    # then
    assert set((value.name, value.type) for value in values) == {
        ("sys/creation_time", "datetime"),
        ("sys/modification_time", "datetime"),
        ("sys/ping_time", "datetime"),
    }


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))


def assert_items_equal(a: list[AttributeDefinition], b: list[AttributeDefinition]):
    return sorted(a, key=lambda d: d.key()) == sorted(b, key=lambda d: d.key())
