import os
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Generator

import pytest
import pytz
from e2e.data import (
    FILE_SERIES_PATHS,
    FLOAT_SERIES_PATHS,
    HISTOGRAM_SERIES_PATHS,
    PATH,
    STRING_SERIES_PATHS,
    TEST_DATA,
)

from neptune_fetcher.exceptions import NeptuneProjectInaccessible
from neptune_fetcher.internal.filters import (
    _Attribute,
    _Filter,
)
from neptune_fetcher.internal.identifiers import ProjectIdentifier
from neptune_fetcher.internal.retrieval import util
from neptune_fetcher.internal.retrieval.search import (
    ExperimentSysAttrs,
    fetch_experiment_sys_attrs,
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
    experiment_filter = _Filter.name_eq(EXPERIMENT_NAME)
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]

    #  when
    experiment_filter = _Filter.any([_Filter.name_eq(EXPERIMENT_NAME), _Filter.name_eq("experiment_not_found")])
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]


def test_find_experiments_by_name_not_found(client, project):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_filter = _Filter.name_eq("name_not_found")
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == []


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (_Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 1), True),
        (_Filter.ge(_Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (_Filter.gt(_Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (_Filter.le(_Attribute(name=f"{PATH}/int-value", type="int"), 0), True),
        (_Filter.lt(_Attribute(name=f"{PATH}/int-value", type="int"), 0), False),
        (_Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1), True),
        (_Filter.ge(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (_Filter.gt(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (_Filter.le(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), True),
        (_Filter.lt(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0), False),
        (_Filter.eq(_Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/str-value", type="string"), "hello_0"), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/str-value", type="string"), "hello_0"), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), True),
        (_Filter.matches_all(_Attribute(name=f"{PATH}/str-value", type="string"), "^he..o_0$"), True),
        (_Filter.matches_all(_Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "lo_0$"]), True),
        (_Filter.matches_all(_Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "y"]), False),
        (_Filter.matches_none(_Attribute(name=f"{PATH}/str-value", type="string"), "x"), True),
        (_Filter.matches_none(_Attribute(name=f"{PATH}/str-value", type="string"), ["x", "y"]), True),
        (_Filter.matches_none(_Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "y"]), False),
        (_Filter.contains_all(_Attribute(name=f"{PATH}/str-value", type="string"), "ll"), True),
        (_Filter.contains_all(_Attribute(name=f"{PATH}/str-value", type="string"), ["e", "ll"]), True),
        (_Filter.contains_all(_Attribute(name=f"{PATH}/str-value", type="string"), ["he", "y"]), False),
        (_Filter.contains_none(_Attribute(name=f"{PATH}/str-value", type="string"), "x"), True),
        (_Filter.contains_none(_Attribute(name=f"{PATH}/str-value", type="string"), ["x", "y"]), True),
        (_Filter.contains_none(_Attribute(name=f"{PATH}/str-value", type="string"), ["he", "y"]), False),
        (_Filter.eq(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (_Filter.eq(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE2), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (_Filter.ne(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE2), True),
        (_Filter.ge(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (_Filter.gt(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (_Filter.le(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (_Filter.lt(_Attribute(name=f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (_Filter.exists(_Attribute(name=f"{PATH}/str-value", type="string")), True),
        (_Filter.exists(_Attribute(name=f"{PATH}/str-value", type="int")), False),
        (_Filter.exists(_Attribute(name=f"{PATH}/does-not-exist-value", type="string")), False),
        (_Filter.exists(_Attribute(name=f"{PATH}/files/file-value.txt", type="file")), True),
        (_Filter.exists(_Attribute(name=f"{PATH}/files/file-value.txt", type="int")), False),
        (_Filter.exists(_Attribute(name=f"{PATH}/files/does-not-exist-value.txt", type="file")), False),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(pytz.timezone("CET")),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                (DATETIME_VALUE + ONE_SECOND).astimezone(pytz.timezone("CET")),
            ),
            False,
        ),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(pytz.timezone("EST")),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                (DATETIME_VALUE + ONE_SECOND).astimezone(pytz.timezone("EST")),
            ),
            False,
        ),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
                DATETIME_VALUE.astimezone(SYSTEM_TZ).replace(tzinfo=None),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=f"{PATH}/datetime-value", type="datetime"),
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
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-2],
            ),
            False,
        ),
        (
            _Filter.ne(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            False,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            _Filter.ne(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            _Filter.ne(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES) + 1.0,
            ),
            False,
        ),
        (
            _Filter.ne(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            _Filter.all(
                [
                    _Filter.ge(
                        _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                        _variance(FLOAT_SERIES_VALUES) - 1e-6,
                    ),
                    _Filter.le(
                        _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                        _variance(FLOAT_SERIES_VALUES) + 1e-6,
                    ),
                ]
            ),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                _variance(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            _Filter.negate(
                _Filter.all(
                    [
                        _Filter.ge(
                            _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                            _variance(FLOAT_SERIES_VALUES) - 1e-6,
                        ),
                        _Filter.le(
                            _Attribute(name=FLOAT_SERIES_PATHS[0], type="float_series", aggregation="variance"),
                            _variance(FLOAT_SERIES_VALUES) + 1e-6,
                        ),
                    ]
                ),
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
            _Filter.exists(
                _Attribute(name=STRING_SERIES_PATHS[0], type="string_series"),
            ),
            True,
        ),
        (
            _Filter.eq(_Attribute(name=STRING_SERIES_PATHS[0], type="string_series"), STRING_SERIES_VALUES[-1]),
            True,
        ),
        (
            _Filter.eq(
                _Attribute(name=STRING_SERIES_PATHS[0], type="string_series"),
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
        # TODO: histogram_series type not supported in nql yet
        # (
        #         _Filter.exists(
        #             _Attribute(name=FILE_SERIES_PATHS[0], type="file_series"),
        #         ),
        #         True,
        # ),
        (
            _Filter.exists(
                _Attribute(name=FILE_SERIES_PATHS[0], type="string_series"),
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_file_series_values(client, project, run_with_attributes, experiment_filter, found):
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
        # TODO: histogram_series type not supported in nql yet
        # (
        #         _Filter.exists(
        #             _Attribute(name=HISTOGRAM_SERIES_PATHS[0], type="histogram_series"),
        #         ),
        #         True,
        # ),
        (
            _Filter.exists(
                _Attribute(name=HISTOGRAM_SERIES_PATHS[0], type="string_series"),
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_histogram_series_values(client, project, run_with_attributes, experiment_filter, found):
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
            _Filter.all(
                [
                    _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            True,
        ),
        (
            _Filter.all(
                [
                    _Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            False,
        ),
        (
            _Filter.all(
                [
                    _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            False,
        ),
        (
            _Filter.all(
                [
                    _Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            False,
        ),
        (
            _Filter.any(
                [
                    _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            True,
        ),
        (
            _Filter.any(
                [
                    _Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            True,
        ),
        (
            _Filter.any(
                [
                    _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            True,
        ),
        (
            _Filter.any(
                [
                    _Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                    _Filter.ne(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                ]
            ),
            False,
        ),
        (
            _Filter.negate(
                _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
            ),
            False,
        ),
        (
            _Filter.negate(
                _Filter.ne(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
            ),
            True,
        ),
        (
            _Filter.all(
                [
                    _Filter.any(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                    _Filter.any(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                        ]
                    ),
                ]
            ),
            True,
        ),
        (
            _Filter.all(
                [
                    _Filter.any(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                    _Filter.any(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                ]
            ),
            False,
        ),
        (
            _Filter.any(
                [
                    _Filter.all(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.0),
                        ]
                    ),
                    _Filter.all(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                ]
            ),
            True,
        ),
        (
            _Filter.any(
                [
                    _Filter.all(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                    _Filter.all(
                        [
                            _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1),
                            _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                        ]
                    ),
                ]
            ),
            False,
        ),
        (
            _Filter.negate(
                _Filter.any(
                    [
                        _Filter.all(
                            [
                                _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 0),
                                _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                            ]
                        ),
                        _Filter.all(
                            [
                                _Filter.eq(_Attribute(name=f"{PATH}/int-value", type="int"), 1),
                                _Filter.eq(_Attribute(name=f"{PATH}/float-value", type="float"), 0.1),
                            ]
                        ),
                    ]
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
            sort_by=_Attribute("sys/name", type="string"),
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
            sort_by=_Attribute("sys/name", type="string"),
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
            sort_by=_Attribute(f"{PATH}/float-series-value", type="float_series"),
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
