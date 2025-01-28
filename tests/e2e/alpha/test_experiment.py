import os
from datetime import (
    datetime,
    timezone,
)
from typing import Generator

import pytest

from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import util
from neptune_fetcher.alpha.internal.experiment import (
    ExperimentSysAttrs,
    fetch_experiment_sys_attrs,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = "pye2e-fetcher-test-experiment"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
DATETIME_VALUE2 = datetime(2025, 2, 1, 0, 0, 0, 0, timezone.utc)
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
        "test/int-value": 10,
        "test/float-value": 0.5,
        "test/str-value": "hello",
        "test/bool-value": True,
        "test/datetime-value": DATETIME_VALUE,
    }
    run.log_configs(data)

    path = "test/float-series-value"
    for step, value in zip(FLOAT_SERIES_STEPS, FLOAT_SERIES_VALUES):
        run.log_metrics(data={path: value}, step=step)

    run.wait_for_processing()

    return run


def test_find_experiments_no_filter(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter=None))

    # then
    assert len(experiment_names) > 0


def test_find_experiments_by_name(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_filter = ExperimentFilter.name_eq(EXPERIMENT_NAME)
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]

    #  when
    experiment_filter = ExperimentFilter.name_in(EXPERIMENT_NAME, "experiment_not_found")
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == [EXPERIMENT_NAME]


def test_find_experiments_by_name_not_found(client, project):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_filter = ExperimentFilter.name_eq("name_not_found")
    experiment_names = _extract_names(fetch_experiment_sys_attrs(client, project_identifier, experiment_filter))

    # then
    assert experiment_names == []


@pytest.mark.parametrize(
    "experiment_filter,found",
    [
        (ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10), True),
        (ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11), False),
        (ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10), False),
        (ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 11), True),
        (ExperimentFilter.ge(Attribute(name="test/int-value", type="int"), 10), True),
        (ExperimentFilter.gt(Attribute(name="test/int-value", type="int"), 10), False),
        (ExperimentFilter.le(Attribute(name="test/int-value", type="int"), 10), True),
        (ExperimentFilter.lt(Attribute(name="test/int-value", type="int"), 10), False),
        (ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5), True),
        (ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6), False),
        (ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.5), False),
        (ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.6), True),
        (ExperimentFilter.ge(Attribute(name="test/float-value", type="float"), 0.5), True),
        (ExperimentFilter.gt(Attribute(name="test/float-value", type="float"), 0.5), False),
        (ExperimentFilter.le(Attribute(name="test/float-value", type="float"), 0.5), True),
        (ExperimentFilter.lt(Attribute(name="test/float-value", type="float"), 0.5), False),
        (ExperimentFilter.eq(Attribute(name="test/bool-value", type="bool"), "True"), True),
        (ExperimentFilter.eq(Attribute(name="test/bool-value", type="bool"), "False"), False),
        (ExperimentFilter.ne(Attribute(name="test/bool-value", type="bool"), "True"), False),
        (ExperimentFilter.ne(Attribute(name="test/bool-value", type="bool"), "False"), True),
        (ExperimentFilter.eq(Attribute(name="test/str-value", type="string"), "hello"), True),
        (ExperimentFilter.eq(Attribute(name="test/str-value", type="string"), "hello2"), False),
        (ExperimentFilter.ne(Attribute(name="test/str-value", type="string"), "hello"), False),
        (ExperimentFilter.ne(Attribute(name="test/str-value", type="string"), "hello2"), True),
        (ExperimentFilter.matches_all(Attribute(name="test/str-value", type="string"), "^he..o$"), True),
        (ExperimentFilter.matches_all(Attribute(name="test/str-value", type="string"), ["^he", "lo$"]), True),
        (ExperimentFilter.matches_all(Attribute(name="test/str-value", type="string"), ["^he", "y"]), False),
        (ExperimentFilter.matches_none(Attribute(name="test/str-value", type="string"), "x"), True),
        (ExperimentFilter.matches_none(Attribute(name="test/str-value", type="string"), ["x", "y"]), True),
        (ExperimentFilter.matches_none(Attribute(name="test/str-value", type="string"), ["^he", "y"]), False),
        (ExperimentFilter.contains_all(Attribute(name="test/str-value", type="string"), "ll"), True),
        (ExperimentFilter.contains_all(Attribute(name="test/str-value", type="string"), ["e", "ll"]), True),
        (ExperimentFilter.contains_all(Attribute(name="test/str-value", type="string"), ["he", "y"]), False),
        (ExperimentFilter.contains_none(Attribute(name="test/str-value", type="string"), "x"), True),
        (ExperimentFilter.contains_none(Attribute(name="test/str-value", type="string"), ["x", "y"]), True),
        (ExperimentFilter.contains_none(Attribute(name="test/str-value", type="string"), ["he", "y"]), False),
        (ExperimentFilter.eq(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (ExperimentFilter.eq(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE2), False),
        (ExperimentFilter.ne(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (ExperimentFilter.ne(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE2), True),
        (ExperimentFilter.ge(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (ExperimentFilter.gt(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (ExperimentFilter.le(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), True),
        (ExperimentFilter.lt(Attribute(name="test/datetime-value", type="datetime"), DATETIME_VALUE), False),
        (ExperimentFilter.exists(Attribute(name="test/str-value", type="string")), True),
        (ExperimentFilter.exists(Attribute(name="test/str-value", type="int")), False),
        (ExperimentFilter.exists(Attribute(name="test/does-not-exist-value", type="string")), False),
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
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            True,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-2],
            ),
            False,
        ),
        (
            ExperimentFilter.ne(
                Attribute(name="test/float-series-value", type="float_series", aggregation="last"),
                FLOAT_SERIES_VALUES[-1],
            ),
            False,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            ExperimentFilter.ne(
                Attribute(name="test/float-series-value", type="float_series", aggregation="min"),
                min(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES) + 1,
            ),
            False,
        ),
        (
            ExperimentFilter.ne(
                Attribute(name="test/float-series-value", type="float_series", aggregation="max"),
                max(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            True,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES) + 1.0,
            ),
            False,
        ),
        (
            ExperimentFilter.ne(
                Attribute(name="test/float-series-value", type="float_series", aggregation="average"),
                sum(FLOAT_SERIES_VALUES) / len(FLOAT_SERIES_VALUES),
            ),
            False,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="variance"), 721.05
            ),
            True,
        ),
        (
            ExperimentFilter.eq(
                Attribute(name="test/float-series-value", type="float_series", aggregation="variance"), 721.05 + 1
            ),
            False,
        ),
        (
            ExperimentFilter.ne(
                Attribute(name="test/float-series-value", type="float_series", aggregation="variance"), 721.05
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
            ExperimentFilter.all(
                ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            ExperimentFilter.all(
                ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            ExperimentFilter.all(
                ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            ExperimentFilter.all(
                ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            True,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10),
                ExperimentFilter.ne(Attribute(name="test/float-value", type="float"), 0.5),
            ),
            False,
        ),
        (
            ExperimentFilter.negate(
                ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
            ),
            False,
        ),
        (
            ExperimentFilter.negate(
                ExperimentFilter.ne(Attribute(name="test/int-value", type="int"), 10),
            ),
            True,
        ),
        (
            ExperimentFilter.all(
                ExperimentFilter.any(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
                ExperimentFilter.any(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
                ),
            ),
            True,
        ),
        (
            ExperimentFilter.all(
                ExperimentFilter.any(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
                ExperimentFilter.any(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
            ),
            False,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.all(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.5),
                ),
                ExperimentFilter.all(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
            ),
            True,
        ),
        (
            ExperimentFilter.any(
                ExperimentFilter.all(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
                ExperimentFilter.all(
                    ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11),
                    ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                ),
            ),
            False,
        ),
        (
            ExperimentFilter.negate(
                ExperimentFilter.any(
                    ExperimentFilter.all(
                        ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10),
                        ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
                    ),
                    ExperimentFilter.all(
                        ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 11),
                        ExperimentFilter.eq(Attribute(name="test/float-value", type="float"), 0.6),
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


def test_find_experiments_paging(client, project, run, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    experiment_names = _extract_names(
        fetch_experiment_sys_attrs(client, project_identifier, experiment_filter=None, batch_size=1)
    )

    # then
    assert len(experiment_names) > 1


def _extract_names(pages: Generator[util.Page[ExperimentSysAttrs], None, None]) -> list[str]:
    return [item.sys_name for page in pages for item in page.items]
