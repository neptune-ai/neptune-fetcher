import os
import re
import time
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_fetcher.internal.composition.attributes import fetch_attribute_definitions
from neptune_fetcher.internal.filters import (
    _AttributeFilter,
    _AttributeNameFilter,
)
from neptune_fetcher.internal.identifiers import (
    AttributeDefinition,
    RunIdentifier,
)
from tests.e2e.conftest import extract_pages

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-01-31"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-composition-attributes-{TEST_DATA_VERSION}"
COMMON_PATH = f"test/test-internal-composition-attributes-{TEST_DATA_VERSION}"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.internal import identifiers
    from neptune_fetcher.internal.filters import _Filter
    from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    force_data_generation = os.getenv("NEPTUNE_E2E_FORCE_DATA_GENERATION", "").lower() in ("true", "1", "yes")
    if not force_data_generation:
        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_identifier),
                _Filter.name_eq(EXPERIMENT_NAME),
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
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.internal.filters import _Filter
    from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = _Filter.name_eq(EXPERIMENT_NAME)
    experiment_attrs = extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


def test_fetch_attribute_definitions_filter_or(client, executor, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[f"^{re.escape(COMMON_PATH)}/.*_value_a$"])],
        type_in=["int"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[f"^{re.escape(COMMON_PATH)}/.*_value_b$"])],
        type_in=["float"],
    )

    #  when
    attribute_filter = _AttributeFilter.any([attribute_filter_1, attribute_filter_2])
    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
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
        lambda a, b, c: _AttributeFilter.any([a, b, c]),
        lambda a, b, c: _AttributeFilter.any([a, _AttributeFilter.any([b, c])]),
    ],
)
def test_fetch_attribute_definitions_filter_triple_or(
    client, executor, project, experiment_identifier, make_attribute_filter
):
    # given
    project_identifier = project.project_identifier

    attribute_filter_1 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[f"^{re.escape(COMMON_PATH)}/.*_value_a$"])],
        type_in=["int"],
    )
    attribute_filter_2 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[f"^{re.escape(COMMON_PATH)}/.*_value_b$"])],
        type_in=["float"],
    )
    attribute_filter_3 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=[f"^{re.escape(COMMON_PATH)}/.*_value_b$"])],
        type_in=["int"],
    )
    attribute_filter = make_attribute_filter(attribute_filter_1, attribute_filter_2, attribute_filter_3)

    #  when
    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            executor=executor,
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


def test_fetch_attribute_definitions_paging_executor(client, executor, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )

    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=executor,
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


def test_fetch_attribute_definitions_should_deduplicate_items(client, executor, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    #  when
    attribute_filter_0 = _AttributeFilter(
        must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
        type_in=["datetime"],
    )
    attribute_filter = attribute_filter_0
    for i in range(10):
        attribute_filter = _AttributeFilter.any(
            [
                attribute_filter,
                _AttributeFilter(
                    must_match_any=[_AttributeNameFilter(must_match_regexes=["sys/.*_time"])],
                    type_in=["datetime"],
                ),
            ]
        )

    attributes = extract_pages(
        fetch_attribute_definitions(
            client,
            [project_identifier],
            [experiment_identifier],
            attribute_filter=attribute_filter,
            batch_size=1,
            executor=executor,
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


def assert_items_equal(a: list[AttributeDefinition], b: list[AttributeDefinition]):
    assert sorted(a, key=lambda d: (d.name, d.type)) == sorted(b, key=lambda d: (d.name, d.type))
