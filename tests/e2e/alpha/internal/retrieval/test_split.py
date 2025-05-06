import os

import pytest

from neptune_fetcher.alpha.exceptions import (
    NeptuneRetryError,
    NeptuneUnexpectedResponseError,
)
from neptune_fetcher.alpha.filters import AttributeFilter
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import (
    AttributeDefinition,
    fetch_attribute_definitions_single_filter,
)
from neptune_fetcher.alpha.internal.retrieval.attribute_values import (
    AttributeValue,
    fetch_attribute_values,
)
from neptune_fetcher.alpha.internal.retrieval.metrics import (
    AttributePathInRun,
    fetch_multiple_series_values,
)
from neptune_fetcher.alpha.internal.retrieval.series import (
    RunAttributeDefinition,
    StringSeriesValue,
    fetch_series_values,
)
from tests.e2e.alpha.internal.conftest import extract_pages
from tests.e2e.alpha.internal.data import (
    NOW,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
LONG_PATH_CONFIGS = TEST_DATA.experiments[0].long_path_configs
LONG_PATH_SERIES = TEST_DATA.experiments[0].long_path_series
LONG_PATH_METRICS = TEST_DATA.experiments[0].long_path_metrics


@pytest.mark.parametrize(
    "limit,success",
    [
        (257, True),
        (258, False),
    ],
)
def test_fetch_attribute_definitions(client, project, experiment_identifier, limit, success):
    # given
    data = dict(list(LONG_PATH_CONFIGS.items())[:limit])
    project_identifier = project.project_identifier

    #  when
    attribute_filter = AttributeFilter(name_eq=list(data.keys()))

    result = None
    thrown_e = None
    try:
        result = extract_pages(
            fetch_attribute_definitions_single_filter(
                client, [project_identifier], [experiment_identifier], attribute_filter=attribute_filter
            )
        )
    except NeptuneUnexpectedResponseError as e:
        thrown_e = e

    # then
    if success:
        assert result == [AttributeDefinition(key, "int") for key in data.keys()]
    else:
        assert thrown_e is not None


@pytest.mark.parametrize(
    "limit,success",
    [
        (len(LONG_PATH_CONFIGS), True),  # no known limit
    ],
)
def test_fetch_attribute_values(client, project, experiment_identifier, limit, success):
    # given
    data = dict(list(LONG_PATH_CONFIGS.items())[:limit])
    project_identifier = project.project_identifier
    attribute_definitions = [AttributeDefinition(key, "int") for key in data.keys()]

    #  when
    result = extract_pages(
        fetch_attribute_values(client, project_identifier, [experiment_identifier], attribute_definitions)
    )

    # then
    assert result == [
        AttributeValue(AttributeDefinition(key, "int"), value=value, run_identifier=experiment_identifier)
        for key, value in data.items()
    ]


@pytest.mark.parametrize(
    "limit,success",
    [
        (len(LONG_PATH_SERIES), True),  # no known limit
    ],
)
def test_fetch_string_series_values(client, project, experiment_identifier, limit, success):
    #  given
    data = dict(list(LONG_PATH_SERIES.items())[:limit])
    attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=experiment_identifier, attribute_definition=AttributeDefinition(key, "string_series")
        )
        for key in data.keys()
    ]

    # when
    result = None
    thrown_e = None
    try:
        result = extract_pages(
            fetch_series_values(
                client, attribute_definitions, include_inherited=True, step_range=(None, None), tail_limit=None
            )
        )
    except NeptuneUnexpectedResponseError as e:
        thrown_e = e

    # then
    if success:
        assert sorted(result, key=lambda r: r[0].attribute_definition.name) == [
            (run_attr, [StringSeriesValue(1.0, value, int(NOW.timestamp() * 1000))])
            for run_attr, value in zip(attribute_definitions, data.values())
        ]
    else:
        assert thrown_e is not None


@pytest.mark.parametrize(
    "limit,success",
    [
        (len(LONG_PATH_METRICS), True),  # no known limit
    ],
)
def test_fetch_float_series_values(client, project, experiment_identifier, limit, success):
    #  given
    data = dict(list(LONG_PATH_METRICS.items())[:limit])
    attribute_definitions = [
        AttributePathInRun(run_identifier=experiment_identifier, run_label="label", attribute_path=key)
        for key in data.keys()
    ]

    # when
    result = None
    thrown_e = None
    try:
        result = fetch_multiple_series_values(
            client,
            attribute_definitions,
            include_inherited=True,
            include_preview=False,
            step_range=(None, None),
            tail_limit=None,
        )
    except NeptuneRetryError as e:
        thrown_e = e

    # then
    if success:
        assert set(result) == {
            ("label", key, int(NOW.timestamp() * 1000), 1.0, value, False, 1.0) for key, value in data.items()
        }
    else:
        assert thrown_e is not None
