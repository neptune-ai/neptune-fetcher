import os

import pytest

from neptune_fetcher import alpha as npt
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
    PATH,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
LONG_PATH_CONFIGS = TEST_DATA.experiments[0].long_path_configs
LONG_PATH_SERIES = TEST_DATA.experiments[0].long_path_series
LONG_PATH_METRICS = TEST_DATA.experiments[0].long_path_metrics


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_CONFIGS), True),  # no known limit
        (2, len(LONG_PATH_CONFIGS), True),
        (3, len(LONG_PATH_CONFIGS), True),
    ],
)
def test_fetch_attribute_definitions_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    # given
    exp_data = experiment_identifiers[:exp_limit]
    attr_data = dict(list(LONG_PATH_CONFIGS.items())[:attr_limit])
    project_identifier = project.project_identifier

    #  when
    result = None
    thrown_e = None
    try:
        result = extract_pages(
            fetch_attribute_definitions_single_filter(
                client, [project_identifier], exp_data, attribute_filter=_attribute_filter("int-value", attr_limit)
            )
        )
    except NeptuneUnexpectedResponseError as e:
        thrown_e = e

    # then
    if success:
        assert thrown_e is None
        assert set(result) == {AttributeDefinition(key, "int") for key in attr_data}
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_CONFIGS)),
        (2, len(LONG_PATH_CONFIGS)),
        (3, len(LONG_PATH_CONFIGS)),
    ],
)
def test_fetch_attribute_definitions_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    # given
    exp_names = TEST_DATA.experiment_names[:exp_limit]
    attr_data = dict(list(LONG_PATH_CONFIGS.items())[:attr_limit])

    #  when
    result = npt.list_attributes(
        experiments=exp_names,
        attributes=_attribute_filter("int-value", attr_limit),
    )

    # then
    assert set(result) == set(attr_data.keys())


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_CONFIGS), True),  # no known limit
        (2, len(LONG_PATH_CONFIGS), True),
        (3, len(LONG_PATH_CONFIGS), True),
    ],
)
def test_fetch_attribute_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    # given
    exp_data = experiment_identifiers[:exp_limit]
    attr_data = dict(list(LONG_PATH_CONFIGS.items())[:attr_limit])
    project_identifier = project.project_identifier
    attribute_definitions = [AttributeDefinition(key, "int") for key in attr_data]

    #  when
    result = None
    thrown_e = None
    try:
        result = extract_pages(fetch_attribute_values(client, project_identifier, exp_data, attribute_definitions))
    except (NeptuneRetryError, NeptuneUnexpectedResponseError) as e:
        thrown_e = e

    # then
    if success:
        assert thrown_e is None
        assert set(result) == {
            AttributeValue(AttributeDefinition(key, "int"), value=value, run_identifier=exp)
            for exp in exp_data
            for key, value in attr_data.items()
        }
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_CONFIGS), True),
        (2, len(LONG_PATH_CONFIGS), True),
        (3, len(LONG_PATH_CONFIGS), True),
    ],
)
def test_fetch_attribute_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    # given
    exp_names = TEST_DATA.experiment_names[:exp_limit]
    attr_data = dict(list(LONG_PATH_CONFIGS.items())[:attr_limit])

    #  when
    result = npt.fetch_experiments_table(
        experiments=exp_names,
        attributes=_attribute_filter("int-value", attr_limit),
        sort_direction="asc",
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == exp_names
    assert result.columns.tolist() == [(attr, "") for attr in attr_data.keys()]


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_SERIES), True),  # no known limit
        (2, 2043, True),
        (2, len(LONG_PATH_SERIES), False),
        (3, 2043, True),
        (3, len(LONG_PATH_SERIES), False),
    ],
)
def test_fetch_string_series_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    #  given
    exp_data = experiment_identifiers[:exp_limit]
    attr_data = dict(list(LONG_PATH_SERIES.items())[:attr_limit])
    attribute_definitions = [
        RunAttributeDefinition(run_identifier=exp, attribute_definition=AttributeDefinition(key, "string_series"))
        for exp in exp_data
        for key in attr_data
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
    except (NeptuneRetryError, NeptuneUnexpectedResponseError) as e:
        thrown_e = e

    # then
    if success:
        expected_values = [
            (
                RunAttributeDefinition(
                    run_identifier=exp, attribute_definition=AttributeDefinition(key, "string_series")
                ),
                [StringSeriesValue(1.0, value, int(NOW.timestamp() * 1000))],
            )
            for exp in exp_data
            for key, value in attr_data.items()
        ]

        assert thrown_e is None
        assert sorted(result, key=lambda r: (r[0].run_identifier.sys_id, r[0].attribute_definition.name)) == sorted(
            expected_values, key=lambda r: (r[0].run_identifier.sys_id, r[0].attribute_definition.name)
        )
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_SERIES)),
        (2, len(LONG_PATH_SERIES)),
        (3, len(LONG_PATH_SERIES)),
    ],
)
def test_fetch_string_series_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    #  given
    exp_names = TEST_DATA.experiment_names[:exp_limit]
    attr_data = dict(list(LONG_PATH_SERIES.items())[:attr_limit])

    # when
    result = npt.fetch_series(
        experiments=exp_names,
        attributes=_attribute_filter("string-series", attr_limit),
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == [(exp, 1.0) for exp in exp_names]
    assert result.columns.tolist() == list(attr_data.keys())


@pytest.mark.parametrize(
    "exp_limit,attr_limit,success",
    [
        (1, len(LONG_PATH_METRICS), True),  # no known limit
        (2, len(LONG_PATH_METRICS), True),
        (3, len(LONG_PATH_METRICS), True),
    ],
)
def test_fetch_float_series_values_retrieval(client, project, experiment_identifiers, exp_limit, attr_limit, success):
    #  given
    exp_data = experiment_identifiers[:exp_limit]
    attr_data = dict(list(LONG_PATH_METRICS.items())[:attr_limit])
    attribute_definitions = [
        AttributePathInRun(run_identifier=exp, run_label=exp.sys_id, attribute_path=key)
        for exp in exp_data
        for key in attr_data
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
        expected_values = {
            (exp.sys_id, key, int(NOW.timestamp() * 1000), 1.0, value, False, 1.0)
            for exp in exp_data
            for key, value in attr_data.items()
        }
        assert thrown_e is None
        assert set(result) == expected_values
    else:
        assert result is None
        assert thrown_e is not None


@pytest.mark.parametrize(
    "exp_limit,attr_limit",
    [
        (1, len(LONG_PATH_METRICS)),
        (2, len(LONG_PATH_METRICS)),
        (3, len(LONG_PATH_METRICS)),
    ],
)
def test_fetch_float_series_values_composition(client, project, experiment_identifiers, exp_limit, attr_limit):
    #  given
    exp_names = TEST_DATA.experiment_names[:exp_limit]
    attr_data = dict(list(LONG_PATH_METRICS.items())[:attr_limit])

    # when
    result = npt.fetch_metrics(
        experiments=exp_names,
        attributes=_attribute_filter("float-series", attr_limit),
    )

    # then
    assert result.shape == (exp_limit, attr_limit)
    assert result.index.tolist() == [(exp, 1.0) for exp in exp_names]
    assert result.columns.tolist() == list(attr_data.keys())


def _attribute_filter(name, limit):
    id_regex = "|".join(str(n) for n in range(limit))
    return AttributeFilter(name_matches_all=f"^{PATH}/long/{name}-0+0({id_regex})$")
