import os
from datetime import timedelta

import pytest

from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.series import (
    RunAttributeDefinition,
    fetch_series_values,
)
from tests.e2e.alpha.internal.conftest import extract_pages
from tests.e2e.alpha.internal.data import (
    NOW,
    STRING_SERIES_PATHS,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def test_fetch_series_values_does_not_exist(client, project, experiment_identifier):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition("does-not-exist", "string"))

    #  when
    series = extract_pages(
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
        experiment_identifier, AttributeDefinition(STRING_SERIES_PATHS[0], "string")
    )

    #  when
    series = extract_pages(
        fetch_series_values(
            client,
            [run_definition],
            include_inherited=False,
        )
    )

    # then
    expected = [
        (step, value, int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000)
        for step, value in enumerate(TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]])
    ]
    assert series == [(run_definition, expected)]


@pytest.mark.parametrize(
    "step_range, expected_start, expected_end",
    [
        ((None, None), 0, 10),
        ((1, None), 1, 10),
        ((None, 5), 0, 6),
        ((2, 7), 2, 8),
        ((None, 2), 0, 3),
        ((5, None), 5, 10),
    ],
)
def test_fetch_series_values_single_series_stop_range(
    client, project, experiment_identifier, step_range, expected_start, expected_end
):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition(STRING_SERIES_PATHS[0], "string")
    )

    #  when
    series = extract_pages(
        fetch_series_values(client, [run_definition], include_inherited=False, step_range=step_range)
    )

    # then
    expected = [
        (step, value, int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000)
        for step, value in list(enumerate(TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]))[
            expected_start:expected_end
        ]
    ]
    assert series == [(run_definition, expected)]


@pytest.mark.parametrize(
    "tail_limit",
    [0, 1, 5, 20, 40],
)
def test_fetch_series_values_single_series_tail_limit(client, project, experiment_identifier, tail_limit):
    # given
    run_definition = RunAttributeDefinition(
        experiment_identifier, AttributeDefinition(STRING_SERIES_PATHS[0], "string")
    )

    #  when
    series = extract_pages(
        fetch_series_values(client, [run_definition], include_inherited=False, tail_limit=tail_limit)
    )

    # then
    if tail_limit == 0:
        assert series == []
    else:
        expected = [
            (step, value, int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000)
            for step, value in reversed(
                list(enumerate(TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]))[-tail_limit:]
            )
        ]
        assert series == [(run_definition, expected)]
