import os
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Generator

import pytest
import pytz

from neptune_fetcher.alpha.exceptions import NeptuneProjectInaccessible
from neptune_fetcher.alpha.filters import (
    Attribute,
    Filter,
)
from neptune_fetcher.alpha.internal.identifiers import ProjectIdentifier
from neptune_fetcher.alpha.internal.retrieval import util
from neptune_fetcher.alpha.internal.retrieval.search import (
    ExperimentSysAttrs,
    fetch_experiment_sys_attrs,
)
from tests.e2e.alpha.internal.data import (
    FLOAT_SERIES_PATHS,
    PATH,
    STRING_SERIES_PATHS,
    TEST_DATA,
)

try:
    SYSTEM_TZ = pytz.timezone(datetime.now(timezone.utc).astimezone().tzname())
except pytz.exceptions.UnknownTimeZoneError:
    SYSTEM_TZ = pytz.timezone("Europe/Warsaw")
ONE_SECOND = timedelta(seconds=1)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
DATETIME_VALUE2 = datetime(2025, 2, 1, 0, 0, 0, 0, timezone.utc)
EXPERIMENT_NAME = TEST_DATA.experiment_names[0]
FLOAT_SERIES_VALUES = TEST_DATA.experiments[0].float_series[FLOAT_SERIES_PATHS[0]]
STRING_SERIES_VALUES = TEST_DATA.experiments[0].string_series[STRING_SERIES_PATHS[0]]


def _variance(xs):
    n = len(xs)
    mean = sum(xs) / n
    return sum((x - mean) ** 2 for x in xs) / n


def test_find_experiments_project_does_not_exist(client, project):
    workspace, project = project.project_identifier.split("/")
    project_identifier = ProjectIdentifier(f"{workspace}/does-not-exist")

    with pytest.raises(NeptuneProjectInaccessible):
        _extract_names(fetch_experiment_sys_attrs(client, project_identifier, filter_=None))


def test_find_experiments_workspace_does_not_exist(client, project):
    project_identifier = ProjectIdentifier("this-workspace/does-not-exist")

    with pytest.raises(NeptuneProjectInaccessible):
        _extract_names(fetch_experiment_sys_attrs(client, project_identifier, filter_=None))


def test_find_experiments_no_filter(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, filter_=None))

    # then
    assert len(experiment_names) > 0


def test_find_experiments_by_name(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_filter = Filter.name_eq(EXPERIMENT_NAME)
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]

    #  when
    experiment_filter = Filter.name_in(EXPERIMENT_NAME, "experiment_not_found")
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]


def test_find_experiments_by_name_not_found(client, project):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_filter = Filter.name_eq("name_not_found")
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == []


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1), False),
        (Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 1), True),
        (Filter.ge(Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (Filter.gt(Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (Filter.le(Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (Filter.lt(Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1), False),
        (Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.1), True),
        (Filter.ge(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (Filter.gt(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (Filter.le(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (Filter.lt(Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (Filter.eq(Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), True),
        (Filter.eq(Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), False),
        (Filter.ne(Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), False),
        (Filter.ne(Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), True),
        (Filter.eq(Attribute(name=f"{PATH}/str-value", type="string"), "hello_0"), True),
        (Filter.eq(Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), False),
        (Filter.ne(Attribute(name=f"{PATH}/str-value", type="string"), "hello_0"), False),
        (Filter.ne(Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), True),
        (Filter.matches_all(Attribute(name=f"{PATH}/str-value", type="string"), "^he..o_0$"), True),
        (Filter.matches_all(Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "lo_0$"]), True),
        (Filter.matches_all(Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "y"]), False),
        (Filter.matches_none(Attribute(name=f"{PATH}/str-value", type="string"), "x"), True),
        (Filter.matches_none(Attribute(name=f"{PATH}/str-value", type="string"), ["x", "y"]), True),
        (Filter.matches_none(Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "y"]), False),
        (Filter.contains_all(Attribute(name=f"{PATH}/str-value", type="string"), "ll"), True),
        (Filter.contains_all(Attribute(name=f"{PATH}/str-value", type="string"), ["e", "ll"]), True),
        (Filter.contains_all(Attribute(name=f"{PATH}/str-value", type="string"), ["he", "y"]), False),
        (Filter.contains_none(Attribute(name=f"{PATH}/str-value", type="string"), "x"), True),
        (Filter.contains_none(Attribute(name=f"{PATH}/str-value", type="string"), ["x", "y"]), True),
        (Filter.contains_none(Attribute(name=f"{PATH}/str-value", type="string"), ["he", "y"]), False),
        (Filter.eq(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (Filter.eq(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE2), False),
        (Filter.ne(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (Filter.ne(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE2), True),
        (Filter.ge(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (Filter.gt(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (Filter.le(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (Filter.lt(Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (Filter.exists(Attribute(name=f"{PATH}/str-value", type="string")), True),
        (Filter.exists(Attribute(name=f"{PATH}/str-value", type="int")), False),
        (Filter.exists(Attribute(name=f"{PATH}/does-not-exist-value", type="string")), False),
        (Filter.exists(Attribute(name=f"{PATH}/files/file-value.txt", type="file")), True),
        (Filter.exists(Attribute(name=f"{PATH}/files/file-value.txt", type="int")), False),
        (Filter.exists(Attribute(name=f"{PATH}/files/does-not-exist-value.txt", type="file")), False),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(pytz.timezone("CET")),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                (DATETIME_VALUE + ONE_SECOND).astimezone(pytz.timezone("CET")),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(pytz.timezone("EST")),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                (DATETIME_VALUE + ONE_SECOND).astimezone(pytz.timezone("EST")),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(SYSTEM_TZ).replace(tzinfo=None),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                (DATETIME_VALUE + ONE_SECOND).astimezone(SYSTEM_TZ).replace(tzinfo=None),
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_config_values(client, project, run_with_attributes, experiment_filter, found):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    if found:
        assert EXPERIMENT_NAME in experiment_names
    else:
        assert EXPERIMENT_NAME not in experiment_names


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-2],
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES) + 1.0,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.ge(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                _variance(FLOAT_SERIES_VALUES) - 1e-6,
            )
            & Filter.le(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                _variance(FLOAT_SERIES_VALUES) + 1e-6,
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                _variance(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.ge(
                    Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                    _variance(FLOAT_SERIES_VALUES) - 1e-6,
                )
                & Filter.le(
                    Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                    _variance(FLOAT_SERIES_VALUES) + 1e-6,
                )
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_float_series_values(client, project, run_with_attributes, experiment_filter, found):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    if found:
        assert EXPERIMENT_NAME in experiment_names
    else:
        assert EXPERIMENT_NAME not in experiment_names


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (
            Filter.exists(
                Attribute(name=STRING_SERIES_PATHS[0], type="string_series"),
            ),
            True,
        ),
        (
            Filter.eq(Attribute(name=STRING_SERIES_PATHS[0], type="string_series"), STRING_SERIES_VALUES[-1]),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=STRING_SERIES_PATHS[0], type="string_series"),
                STRING_SERIES_VALUES[-2],
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_string_series_values(client, project, run_with_attributes, experiment_filter, found):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    if found:
        assert EXPERIMENT_NAME in experiment_names
    else:
        assert EXPERIMENT_NAME not in experiment_names


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (
            Filter.all(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            False,
        ),
        (
            Filter.all(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            False,
        ),
        (
            Filter.all(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            False,
        ),
        (
            Filter.any(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 0),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
            ),
            False,
        ),
        (
            Filter.any(
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ),
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                ),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.any(
                    Filter.all(
                        Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 0),
                        Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                    ),
                    Filter.all(
                        Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 1),
                        Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                    ),
                )
            ),
            True,
        ),
    ],
)
def test_find_experiments_by_logical_expression(client, project, run_with_attributes, experiment_filter, found):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    if found:
        assert EXPERIMENT_NAME in experiment_names
    else:
        assert EXPERIMENT_NAME not in experiment_names


def test_find_experiments_sort_by_name_desc(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    # when
    experiment_names = _extract_names(
        fetch_experiment_sys_attrs(
            client,
            project_identifier,
            filter_=None,
            sort_by=Attribute("sys/name", type="string"),
            sort_direction="desc",
        )
    )

    # then
    assert len(experiment_names) > 1
    assert experiment_names == sorted(experiment_names, reverse=True)


def test_find_experiments_sort_by_name_asc(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(
        fetch_experiment_sys_attrs(
            client,
            project_identifier,
            filter_=None,
            sort_by=Attribute("sys/name", type="string"),
            sort_direction="asc",
        )
    )

    # then
    assert len(experiment_names) > 1
    assert experiment_names == sorted(experiment_names)


def test_find_experiments_sort_by_aggregate(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(
        fetch_experiment_sys_attrs(
            client,
            project_identifier,
            filter_=None,
            sort_by=Attribute(f"{PATH}/float-series-value", type="float_series"),
        )
    )

    # then
    assert len(experiment_names) > 1
    # TODO: assert order


def test_find_experiments_limit(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, filter_=None, limit=1))

    # then
    assert len(experiment_names) == 1


def _extract_names(pages: Generator[util.Page[ExperimentSysAttrs], None, None]) -> list[str]:
    return [item.sys_name for page in pages for item in page.items]
