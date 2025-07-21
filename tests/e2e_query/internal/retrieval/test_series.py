import os
from datetime import timedelta

import pytest

from neptune_query.internal.identifiers import AttributeDefinition
from neptune_query.internal.retrieval.series import (
    RunAttributeDefinition,
    fetch_series_values,
)
from tests.e2e_query.conftest import extract_pages
from tests.e2e_query.data import (
    FILE_SERIES_PATHS,
    HISTOGRAM_SERIES_PATHS,
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


@pytest.mark.parametrize(
    "attribute_name, attribute_type, expected_values",
    [
        (STRING_SERIES_PATHS[0], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]),
        (STRING_SERIES_PATHS[1], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[1]]),
        (
            HISTOGRAM_SERIES_PATHS[0],
            "histogram_series",
            TEST_DATA.experiments[0].fetcher_histogram_series()[HISTOGRAM_SERIES_PATHS[0]],
        ),
        (FILE_SERIES_PATHS[0], "file_series", TEST_DATA.experiments[0].file_series_matchers()[FILE_SERIES_PATHS[0]]),
    ],
)
def test_fetch_series_values_single_series(
    client, project, experiment_identifier, attribute_name, attribute_type, expected_values
):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition(attribute_name, attribute_type))

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
        for step, value in enumerate(expected_values)
    ]
    assert series == [(run_definition, expected)]


@pytest.mark.parametrize(
    "attribute_name, attribute_type, expected_values",
    [
        (STRING_SERIES_PATHS[0], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]),
        (STRING_SERIES_PATHS[1], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[1]]),
        (
            HISTOGRAM_SERIES_PATHS[0],
            "histogram_series",
            TEST_DATA.experiments[0].fetcher_histogram_series()[HISTOGRAM_SERIES_PATHS[0]],
        ),
        (FILE_SERIES_PATHS[0], "file_series", TEST_DATA.experiments[0].file_series_matchers()[FILE_SERIES_PATHS[0]]),
    ],
)
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
    client,
    project,
    experiment_identifier,
    attribute_name,
    attribute_type,
    expected_values,
    step_range,
    expected_start,
    expected_end,
):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition(attribute_name, attribute_type))

    #  when
    series = extract_pages(
        fetch_series_values(client, [run_definition], include_inherited=False, step_range=step_range)
    )

    # then
    expected = [
        (step, value, int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000)
        for step, value in list(enumerate(expected_values))[expected_start:expected_end]
    ]
    if expected:
        assert series == [(run_definition, expected)]
    else:
        assert series == []


@pytest.mark.parametrize(
    "attribute_name, attribute_type, expected_values",
    [
        (STRING_SERIES_PATHS[0], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]),
        (STRING_SERIES_PATHS[1], "string_series", TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[1]]),
        (
            HISTOGRAM_SERIES_PATHS[0],
            "histogram_series",
            TEST_DATA.experiments[0].fetcher_histogram_series()[HISTOGRAM_SERIES_PATHS[0]],
        ),
        (FILE_SERIES_PATHS[0], "file_series", TEST_DATA.experiments[0].file_series_matchers()[FILE_SERIES_PATHS[0]]),
    ],
)
@pytest.mark.parametrize(
    "tail_limit",
    [0, 1, 5, 20, 40],
)
def test_fetch_series_values_single_series_tail_limit(
    client, project, experiment_identifier, attribute_name, attribute_type, expected_values, tail_limit
):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition(attribute_name, attribute_type))

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
            for step, value in reversed(list(enumerate(expected_values))[-tail_limit:])
        ]
        assert series == [(run_definition, expected)]
