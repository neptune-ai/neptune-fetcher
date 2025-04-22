import datetime
import itertools as it
import os

import pytest

from neptune_fetcher.alpha.internal.identifiers import RunIdentifier
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.series import (
    RunAttributeDefinition,
    fetch_series_values,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-04-21"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-retrieval-series-{TEST_DATA_VERSION}"
COMMON_PATH = f"test/test-internal-retrieval-series-{TEST_DATA_VERSION}"
STRING_SERIES_STEPS = [step * 0.5 for step in range(20)]
STRING_SERIES_TIMESTAMPS = [datetime.datetime(2025, 2, 21, 10, step) for step in range(20)]
STRING_SERIES_VALUES = [chr(ord("a") + step) * (step % 3 + 1) for step in range(20)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.alpha.internal import identifiers
    from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            Filter.name_in(EXPERIMENT_NAME),
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

    path = f"{COMMON_PATH}/string-series-value"
    for step, timestamp, value in zip(STRING_SERIES_STEPS, STRING_SERIES_TIMESTAMPS, STRING_SERIES_VALUES):
        run.log_string_series(data={path: value}, step=step, timestamp=timestamp)

    run.close()

    return run


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = Filter.name_in(EXPERIMENT_NAME)
    experiment_attrs = _extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


def test_fetch_series_values_does_not_exist(client, project, experiment_identifier):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition("does-not-exist", "string"))

    #  when
    series = _extract_pages(
        fetch_series_values(
            client,
            [run_definition],
            include_inherited=False,
        )
    )

    # then
    assert series == []


def test_fetch_series_values_single_series(client, project, experiment_identifier):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition(f"{COMMON_PATH}/string-series-value", "string")
    )

    #  when
    series = _extract_pages(
        fetch_series_values(
            client,
            [run_definition],
            include_inherited=False,
        )
    )

    # then
    expected = [
        (step, value, int(ts.timestamp() * 1000))
        for step, value, ts in zip(STRING_SERIES_STEPS, STRING_SERIES_VALUES, STRING_SERIES_TIMESTAMPS)
    ]
    assert series == [(run_definition, expected)]


@pytest.mark.parametrize(
    "step_range, expected_start, expected_end",
    [
        ((None, None), 0, 20),
        ((1, None), 2, 20),
        ((None, 5.0), 0, 11),
        ((2.5, 7.5), 5, 16),
        ((None, 2.5), 0, 6),
        ((5.0, None), 10, 20),
    ],
)
def test_fetch_series_values_single_series_stop_range(
    client, project, experiment_identifier, step_range, expected_start, expected_end
):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition(f"{COMMON_PATH}/string-series-value", "string")
    )

    #  when
    series = _extract_pages(
        fetch_series_values(client, [run_definition], include_inherited=False, step_range=step_range)
    )

    # then
    expected = [
        (step, value, int(ts.timestamp() * 1000))
        for step, value, ts in zip(
            STRING_SERIES_STEPS[expected_start:expected_end],
            STRING_SERIES_VALUES[expected_start:expected_end],
            STRING_SERIES_TIMESTAMPS[expected_start:expected_end],
        )
    ]
    assert series == [(run_definition, expected)]


@pytest.mark.parametrize(
    "tail_limit",
    [0, 1, 5, 20, 40],
)
def test_fetch_series_values_single_series_tail_limit(client, project, experiment_identifier, tail_limit):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition(f"{COMMON_PATH}/string-series-value", "string")
    )

    #  when
    series = _extract_pages(
        fetch_series_values(client, [run_definition], include_inherited=False, tail_limit=tail_limit)
    )

    # then
    if tail_limit == 0:
        assert series == []
    else:
        expected = [
            (step, value, int(ts.timestamp() * 1000))
            for step, value, ts in reversed(
                list(
                    zip(
                        STRING_SERIES_STEPS[-tail_limit:],
                        STRING_SERIES_VALUES[-tail_limit:],
                        STRING_SERIES_TIMESTAMPS[-tail_limit:],
                    )
                )
            )
        ]
        assert series == [(run_definition, expected)]


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
