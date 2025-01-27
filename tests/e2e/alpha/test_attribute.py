import os
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_fetcher.alpha.filter import AttributeFilter
from neptune_fetcher.alpha.internal.attribute import (
    AttributeDefinition,
    fetch_attribute_definitions,
)
from neptune_fetcher.alpha.internal.identifiers import ExperimentIdentifier

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
    experiment_attrs = fetch_experiment_sys_attrs(
        client, project_identifier=project_identifier, experiment_filter=experiment_filter
    )
    sys_id = list(experiment_attrs)[0].items[0].sys_id

    return ExperimentIdentifier(project_identifier, sys_id)


def test_find_attributes_single_string(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_find_attributes_does_not_exist(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="does-not-exist", type_in=["string"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert attributes == []


def test_find_attributes_two_strings(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq=["sys/name", "sys/owner"], type_in=["string"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == {AttributeDefinition("sys/name", "string"), AttributeDefinition("sys/owner", "string")}


def test_find_attributes_single_series(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    path = f"{COMMON_PATH}/float-series-value"

    #  when
    attribute_filter = AttributeFilter(name_eq=path, type_in=["float_series"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert attributes == [AttributeDefinition(path, "float_series")]


def test_find_attributes_all_types(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    all_attrs = [
        AttributeDefinition(f"{COMMON_PATH}/int-value", "int"),
        AttributeDefinition(f"{COMMON_PATH}/float-value", "float"),
        AttributeDefinition(f"{COMMON_PATH}/str-value", "string"),
        AttributeDefinition(f"{COMMON_PATH}/bool-value", "bool"),
        AttributeDefinition(f"{COMMON_PATH}/datetime-value", "datetime"),
        AttributeDefinition(f"{COMMON_PATH}/float-series-value", "float_series"),
        AttributeDefinition("sys/tags", "string_set"),
    ]

    #  when
    attribute_filter = AttributeFilter(name_eq=[attr.name for attr in all_attrs])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == set(all_attrs)


def test_find_attributes_no_type_in(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name")
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_find_attributes_regex_matches_all(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/modification_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }


def test_find_attributes_regex_matches_none(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(
        name_matches_all="sys/.*_time", name_matches_none="modification", type_in=["datetime"]
    )
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }


def test_find_attributes_multiple_projects(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier
    project_identifier_2 = f"{project_identifier}-does-not-exist"

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attributes = fetch_attribute_definitions(
        client,
        [project_identifier, project_identifier, project_identifier_2],
        [experiment_identifier],
        attribute_filter=attribute_filter,
    )

    # then
    assert attributes == [AttributeDefinition("sys/name", "string")]


def test_find_attributes_filter_or(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["float"])

    #  when
    attribute_filter = attribute_filter_1 | attribute_filter_2
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == {
        AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int"),
        AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float"),
    }


@pytest.mark.parametrize(
    "make_attribute_filter",
    [
        lambda a, b, c: a | b | c,
        lambda a, b, c: AttributeFilter.any(a, b, c),
        lambda a, b, c: AttributeFilter.any(a, AttributeFilter.any(b, c)),
    ],
)
def test_find_attributes_filter_triple_or(client, project, experiment_identifier, make_attribute_filter):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["float"])
    attribute_filter_3 = AttributeFilter(name_matches_all=f"^{re.escape(COMMON_PATH)}/.*_value_b$", type_in=["int"])
    attribute_filter = make_attribute_filter(attribute_filter_1, attribute_filter_2, attribute_filter_3)

    #  when
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
    )

    # then
    assert set(attributes) == {
        AttributeDefinition(f"{COMMON_PATH}/int_value_a", "int"),
        AttributeDefinition(f"{COMMON_PATH}/int_value_b", "int"),
        AttributeDefinition(f"{COMMON_PATH}/float_value_b", "float"),
    }


def test_find_attributes_paging(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attributes = fetch_attribute_definitions(
        client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter, batch_size=1
    )

    # then
    assert set(attributes) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/modification_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }


def test_find_attributes_paging_executor(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    with ThreadPoolExecutor(max_workers=2) as executor:
        attributes = fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=executor,
        )

    # then
    assert set(attributes) == {
        AttributeDefinition("sys/creation_time", "datetime"),
        AttributeDefinition("sys/modification_time", "datetime"),
        AttributeDefinition("sys/ping_time", "datetime"),
    }
