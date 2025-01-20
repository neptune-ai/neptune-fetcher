import os
from datetime import (
    datetime,
    timezone,
)

import pytest
from neptune_scale import Run

from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.experiment import find_experiments

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")

DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
DATETIME_VALUE2 = datetime(2025, 2, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_VALUES = [(step * 0.5, step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attrs(project, run_init_kwargs):
    run = Run(**run_init_kwargs)

    data = {
        "test/int-value": 10,
        "test/float-value": 0.5,
        "test/str-value": "hello",
        "test/bool-value": True,
        "test/datetime-value": DATETIME_VALUE,
    }
    run.log_configs(data)

    path = "test/float-series-value"
    for step, value in FLOAT_SERIES_VALUES:
        run.log_metrics(data={path: value}, step=step)
    run.wait_for_processing()

    return run


def test_find_experiments_by_name(client, run_init_kwargs):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = run_init_kwargs["experiment_name"]

    #  when
    experiment_filter = f'`sys/name`:string = "{experiment_name}"'
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == [experiment_name]


def test_find_experiments_by_name_not_found(client, run_init_kwargs):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = "test_find_experiments_by_name_not_found"

    #  when
    experiment_filter = f'`sys/name`:string = "{experiment_name}"'
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == []


def test_find_experiments_by_name_filter(client, run_init_kwargs):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = run_init_kwargs["experiment_name"]

    #  when
    experiment_filter = ExperimentFilter.name_eq(experiment_name)
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == [experiment_name]

    #  when
    experiment_filter = ExperimentFilter.name_in(experiment_name, "experiment_not_found")
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == [experiment_name]


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
    ],
)
def test_find_experiments_by_config_values(client, run_init_kwargs, run_with_attrs, experiment_filter, found):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = run_init_kwargs["experiment_name"]

    #  when
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    if found:
        assert experiment_names == [experiment_name]
    else:
        assert experiment_names == []
