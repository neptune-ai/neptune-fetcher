import os
import uuid

import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import (
    Context,
    list_experiments,
)
from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")  # FIXME - this uses non alpha code


EXPERIMENTS = {
    "exp_foo1": {
        "configs": {"optimizer": "adam"},
        "metrics": [
            (1, {"accuracy": 0.5}),
            (2, {"accuracy": 0.6}),
        ],
    },
    "exp_foo2": {
        "configs": {"optimizer": "adam"},
        "metrics": [
            (1, {"accuracy": 0.5}),
            (2, {"accuracy": 0.6}),
        ],
    },
    "unrelated_exp_foo": {
        "configs": {"optimizer": "eve"},
        "metrics": [
            (1, {"accuracy": 0.5}),
            (2, {"accuracy": 0.6}),
        ],
    },
}

EXPERIMENT_NAMES = EXPERIMENTS.keys()


@pytest.fixture(scope="module")
def runs_with_attributes(project):
    project_id = project.project_identifier

    runs = []

    for experiment_name, data in EXPERIMENTS.items():
        run_id = str(uuid.uuid4())
        run = Run(
            project=project_id,
            run_id=run_id,
            experiment_name=experiment_name,
        )

        run.log_configs(data["configs"])

        for step, items in data["metrics"]:
            run.log_metrics(data=items, step=step)

        run.wait_for_processing()
        runs.append(run)

    yield runs

    # delete?


@pytest.fixture
def project_context():
    return Context(project=NEPTUNE_PROJECT)


@pytest.mark.parametrize(
    "experiments, expected",
    [
        ("exp_foo1", ["exp_foo1"]),
        ("^exp_foo", ["exp_foo1", "exp_foo2"]),
        ("exp_foo", ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        ("foo.$", ["exp_foo1", "exp_foo2"]),
        ("foo.+$", ["exp_foo1", "exp_foo2"]),
        ("foo.*$", ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
    ],
)
def test_list_experiments_regex(experiments, expected, runs_with_attributes, project_context):
    result = list_experiments(experiments, context=project_context)
    assert result == expected


@pytest.mark.parametrize(
    "experiments, expected",
    [
        (None, ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        ("", ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        ("1|2", ["exp_foo1", "exp_foo2"]),
    ],
)
def test_list_experiments_regex_subset(experiments, expected, runs_with_attributes, project_context):
    result = list_experiments(experiments, context=project_context)
    assert set(result) >= set(expected)


@pytest.mark.parametrize(
    "experiments, expected",
    [
        (ExperimentFilter.ge("metrics/accuracy", 0.5), ["exp_foo1", "exp_foo2"]),
        (ExperimentFilter.lt("metrics/accuracy", 0.5), ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        (ExperimentFilter.le("metrics/accuracy", 0.5), ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        (ExperimentFilter.eq("metrics/accuracy", 0.5), ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
        (ExperimentFilter.matches_all("config/optimizer", ".*ada.*"), ["exp_foo1", "exp_foo2"]),
        (ExperimentFilter.matches_all("config/optimizer", [".*ada.*", "^eve_.*"]), ["exp_foo1", "exp_foo2"]),
        (ExperimentFilter.matches_none("config/optimizer", ".*ada.*"), ["exp_foo1", "exp_foo2"]),
        (ExperimentFilter.matches_none("config/optimizer", [".*ada.*", "^eve_.*"]), ["exp_foo1", "exp_foo2"]),
        (ExperimentFilter.ge("test/int-value", 10), ["pye2e-fetcher-test-experiment"]),
        (ExperimentFilter.ge("test/int-value:int", 0), ["pye2e-fetcher-test-experiment"]),
        (ExperimentFilter.eq(Attribute(name="test/int-value", type="int"), 10), ["pye2e-fetcher-test-experiment"]),
    ],
)
def test_list_experiments_filter(experiments, expected, project_context):
    result = list_experiments(experiments, context=project_context)
    assert result == expected
