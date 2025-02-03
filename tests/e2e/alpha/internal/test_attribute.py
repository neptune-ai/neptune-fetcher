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
from neptune_fetcher.alpha.internal import util
from neptune_fetcher.alpha.internal.attribute import (
    AttributeDefinition,
    AttributeValue,
    fetch_attribute_definitions,
    fetch_attribute_values,
)
from neptune_fetcher.alpha.internal.identifiers import (
    ExperimentIdentifier,
    SysId,
)
from neptune_fetcher.alpha.internal.types import FloatSeriesAggregations

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-01-31"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-attribute-{TEST_DATA_VERSION}"
COMMON_PATH = f"test/test-internal-attribute-{TEST_DATA_VERSION}"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.filter import ExperimentFilter
    from neptune_fetcher.alpha.internal import identifiers
    from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            ExperimentFilter.name_in(EXPERIMENT_NAME),
        )
    )
    if existing.items:
        return

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

    run.close()

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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_does_not_exist(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="does-not-exist", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/name", "string"),
            AttributeDefinition("sys/owner", "string"),
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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition(path, "float_series")]


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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    expected_definitions = [AttributeDefinition(name, type) for name, type in all_attrs]
    assert_items_equal(attributes, expected_definitions)


def test_fetch_attribute_definitions_no_type_in(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name")
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_regex_matches_all(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
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
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_filter_or(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["float"])

    #  when
    attribute_filter = attribute_filter_1 | attribute_filter_2
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int"),
            AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float"),
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
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int"),
            AttributeDefinition(f"{COMMON_PATH}/int_value_b", "int"),
            AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float"),
        ],
    )


def test_fetch_attribute_definitions_paging(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_paging_executor(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])

    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_should_deduplicate_items(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter_0 = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attribute_filter = attribute_filter_0
    for i in range(10):
        attribute_filter = attribute_filter | AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])

    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert_items_equal(
        attributes,
        [
            AttributeDefinition("sys/creation_time", "datetime"),
            AttributeDefinition("sys/modification_time", "datetime"),
            AttributeDefinition("sys/ping_time", "datetime"),
        ],
    )


def test_fetch_attribute_definitions_experiment_identifier_none(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            None,
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_fetch_attribute_definitions_experiment_identifier_empty(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [],
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == []


def test_fetch_attribute_values_single_string(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition("sys/name", "string")],
        )
    )

    # then
    assert values == [AttributeValue(AttributeDefinition("sys/name", "string"), EXPERIMENT_NAME, experiment_identifier)]


def test_fetch_attribute_values_two_strings(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    values: list[AttributeValue] = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [
                AttributeDefinition("sys/name", "string"),
                AttributeDefinition("sys/owner", "string"),
            ],
        )
    )

    # then
    assert set(value.attribute_definition for value in values) == {
        AttributeDefinition("sys/name", "string"),
        AttributeDefinition("sys/owner", "string"),
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
            [AttributeDefinition(path, "float_series")],
        )
    )

    # then
    average = sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES)
    aggregates = FloatSeriesAggregations(
        last=FLOAT_SERIES_VALUES[-1],
        min=min(FLOAT_SERIES_VALUES),
        max=max(FLOAT_SERIES_VALUES),
        average=average,
        variance=sum((value - average) ** 2 for value in FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
    )
    assert values == [AttributeValue(AttributeDefinition(path, "float_series"), aggregates, experiment_identifier)]


def test_fetch_attribute_values_all_types(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    average = sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES)
    all_values = [
        AttributeValue(AttributeDefinition(f"{COMMON_PATH}/int-value", "int"), 10, experiment_identifier),
        AttributeValue(AttributeDefinition(f"{COMMON_PATH}/float-value", "float"), 0.5, experiment_identifier),
        AttributeValue(AttributeDefinition(f"{COMMON_PATH}/str-value", "string"), "hello", experiment_identifier),
        AttributeValue(AttributeDefinition(f"{COMMON_PATH}/bool-value", "bool"), True, experiment_identifier),
        AttributeValue(
            AttributeDefinition(f"{COMMON_PATH}/datetime-value", "datetime"), DATETIME_VALUE, experiment_identifier
        ),
        AttributeValue(
            AttributeDefinition(f"{COMMON_PATH}/float-series-value", "float_series"),
            FloatSeriesAggregations(
                last=FLOAT_SERIES_VALUES[-1],
                min=min(FLOAT_SERIES_VALUES),
                max=max(FLOAT_SERIES_VALUES),
                average=average,
                variance=sum((value - average) ** 2 for value in FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            experiment_identifier,
        ),
        AttributeValue(AttributeDefinition("sys/tags", "string_set"), {"string-set-item"}, experiment_identifier),
    ]

    #  when
    values = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [value.attribute_definition for value in all_values],
        )
    )

    # then
    assert len(values) == len(all_values)
    for expected in all_values:
        value = next(value for value in values if value.attribute_definition == expected.attribute_definition)
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
                AttributeDefinition("sys/creation_time", "datetime"),
                AttributeDefinition("sys/modification_time", "datetime"),
                AttributeDefinition("sys/ping_time", "datetime"),
            ],
            batch_size=1,
        )
    )

    # then
    assert set(value.attribute_definition for value in values) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/modification_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))


def assert_items_equal(a: list[AttributeDefinition], b: list[AttributeDefinition]):
    assert sorted(a, key=lambda d: (d.name, d.type)) == sorted(b, key=lambda d: (d.name, d.type))


def test_fetch_attribute_definitions_experiment_large_number_experiment_identifiers(
    client, project, experiment_identifier
):
    # given
    project_identifier = project.project_identifier

    experiment_identifiers = [experiment_identifier] + _generate_experiment_identifiers(project_identifier, 240 * 1024)

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = _extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            experiment_identifiers,
            attribute_filter=attribute_filter,
            executor=util.create_thread_pool_executor(),
        )
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def _generate_experiment_identifiers(project_identifier, size_bytes: int):
    per_uuid_size = 50

    identifiers_count = (size_bytes + per_uuid_size) // per_uuid_size

    experiment_identifiers = [
        ExperimentIdentifier(project_identifier, SysId(f"TEST-{i}")) for i in range(identifiers_count)
    ]

    return experiment_identifiers
