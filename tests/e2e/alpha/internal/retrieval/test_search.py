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

SYSTEM_TZ = pytz.timezone(datetime.now(timezone.utc).astimezone().tzname())
ONE_SECOND = timedelta(seconds=1)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-02-03"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-retrieval-search-{TEST_DATA_VERSION}"
PATH = f"test/test-internal-retrieval-search-{TEST_DATA_VERSION}"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
DATETIME_VALUE2 = datetime(2025, 2, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.internal import identifiers

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

    data = {
        f"{PATH}/int-value": 10,
        f"{PATH}/float-value": 0.5,
        f"{PATH}/str-value": "hello",
        f"{PATH}/bool-value": True,
        f"{PATH}/datetime-value": DATETIME_VALUE,
    }
    run.log_configs(data)

    path = f"{PATH}/float-series-value"
    for step, value in zip(FLOAT_SERIES_STEPS, FLOAT_SERIES_VALUES):
        run.log_metrics(data={path: value}, step=step)

    run.close()

    return run


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
        (Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10), True),
        (Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11), False),
        (Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10), False),
        (Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 11), True),
        (Filter.ge(Attribute(name=f"{PATH}/int-value", type="int"), 10), True),
        (Filter.gt(Attribute(name=f"{PATH}/int-value", type="int"), 10), False),
        (Filter.le(Attribute(name=f"{PATH}/int-value", type="int"), 10), True),
        (Filter.lt(Attribute(name=f"{PATH}/int-value", type="int"), 10), False),
        (Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), True),
        (Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6), False),
        (Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), False),
        (Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.6), True),
        (Filter.ge(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), True),
        (Filter.gt(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), False),
        (Filter.le(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), True),
        (Filter.lt(Attribute(name=f"{PATH}/float-value", type="float"), 0.5), False),
        (Filter.eq(Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), True),
        (Filter.eq(Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), False),
        (Filter.ne(Attribute(name=f"{PATH}/bool-value", type="bool"), "True"), False),
        (Filter.ne(Attribute(name=f"{PATH}/bool-value", type="bool"), "False"), True),
        (Filter.eq(Attribute(name=f"{PATH}/str-value", type="string"), "hello"), True),
        (Filter.eq(Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), False),
        (Filter.ne(Attribute(name=f"{PATH}/str-value", type="string"), "hello"), False),
        (Filter.ne(Attribute(name=f"{PATH}/str-value", type="string"), "hello2"), True),
        (Filter.matches_all(Attribute(name=f"{PATH}/str-value", type="string"), "^he..o$"), True),
        (Filter.matches_all(Attribute(name=f"{PATH}/str-value", type="string"), ["^he", "lo$"]), True),
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
        assert experiment_names == [EXPERIMENT_NAME]
    else:
        assert experiment_names == []


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-2],
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES) + 1.0,
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="variance"), 721.05
            ),
            True,
        ),
        (
            Filter.eq(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="variance"), 721.05 + 1
            ),
            False,
        ),
        (
            Filter.ne(
                Attribute(name=f"{PATH}/float-series-value", type="float_series", aggregation="variance"), 721.05
            ),
            False,
        ),
    ],
)
def test_find_experiments_by_series_values(client, project, run_with_attributes, experiment_filter, found):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    if found:
        assert experiment_names == [EXPERIMENT_NAME]
    else:
        assert experiment_names == []


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (
            Filter.all(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            Filter.all(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            Filter.all(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            Filter.any(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                Filter.ne(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.ne(Attribute(name=f"{PATH}/int-value", type="int"), 10),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
                ),
            ),
            True,
        ),
        (
            Filter.all(
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
                Filter.any(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
            ),
            False,
        ),
        (
            Filter.any(
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.5),
                ),
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
            ),
            True,
        ),
        (
            Filter.any(
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
                Filter.all(
                    Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11),
                    Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                ),
            ),
            False,
        ),
        (
            Filter.negate(
                Filter.any(
                    Filter.all(
                        Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 10),
                        Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
                    ),
                    Filter.all(
                        Filter.eq(Attribute(name=f"{PATH}/int-value", type="int"), 11),
                        Filter.eq(Attribute(name=f"{PATH}/float-value", type="float"), 0.6),
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
        assert experiment_names == [EXPERIMENT_NAME]
    else:
        assert experiment_names == []


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
