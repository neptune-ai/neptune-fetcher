import os
import uuid

import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import (
    Context,
    list_experiments,
    set_context,
)
from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.context import get_local_or_global_context

NEPTUNE_PROJECT = os.getenv("NEPTUNE_PROJECT")  # FIXME - this uses non alpha code in the conftest
NEPTUNE_PROJECT_BARE = os.getenv("NEPTUNE_PROJECT_BARE")


EXPERIMENTS_IN_PROJECT1 = {
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

# to be created in NEPTUNE_PROJECT_BARE
EXPERIMENTS_IN_BARE_PROJECT = {
    "xxp_foo1": {
        "configs": {"optimizer": "adam"},
        "metrics": [
            (1, {"accuracy": 0.4}),
            (2, {"accuracy": 0.3}),
        ],
    },
    "xxp_foo2": {
        "configs": {"optimizer": "adam"},
        "metrics": [
            (1, {"accuracy": 0.4}),
            (2, {"accuracy": 0.6}),
        ],
    },
    "unrelated_xxp_foo": {
        "configs": {"optimizer": "eve"},
        "metrics": [
            (1, {"accuracy": 0.8}),
            (2, {"accuracy": 0.7}),
        ],
    },
}


def populate_run(project_id: str, experiment_setup: dict):
    runs = []

    assert project_id, "Test error - project_id is expected to be set!"

    for experiment_name, data in experiment_setup.items():
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

    return runs


@pytest.fixture(scope="module")
def runs_with_attributes(project):
    project_id = project.project_identifier

    runs = populate_run(project_id, EXPERIMENTS_IN_PROJECT1)
    runs.extend(populate_run(NEPTUNE_PROJECT_BARE, EXPERIMENTS_IN_BARE_PROJECT))

    yield runs


@pytest.fixture
def project_context():
    return Context(project=NEPTUNE_PROJECT)


@pytest.fixture
def bare_project_context():
    return Context(project=NEPTUNE_PROJECT_BARE)


@pytest.fixture
def global_context_project1(project_context):
    old = get_local_or_global_context()
    yield set_context(project_context)
    set_context(old)


def global_context_project2(bare_project_context):
    old = get_local_or_global_context()
    yield set_context(bare_project_context)
    set_context(old)


project1 = Context(project=NEPTUNE_PROJECT)
project2 = Context(project=NEPTUNE_PROJECT_BARE)


REGEXES_REGULAR_PROJECT = [
    ("exp_foo1", project1, ["exp_foo1"]),
    ("^exp_foo", project1, ["exp_foo1", "exp_foo2"]),
    ("exp_foo", project1, ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
    ("foo.$", project1, ["exp_foo1", "exp_foo2"]),
    ("foo.+$", project1, ["exp_foo1", "exp_foo2"]),
    ("foo.*$", project1, ["exp_foo1", "exp_foo2", "unrelated_exp_foo"]),
]

REGEXES_BARE_PROJECT = [
    ("exp_foo1", project2, []),
    ("xxp_foo1", project2, ["xxp_foo1"]),
    ("^xxp_foo", project2, ["xxp_foo1", "xxp_foo2"]),
    ("xxp_foo", project2, ["xxp_foo1", "xxp_foo2", "unrelated_xxp_foo"]),
    ("foo.$", project2, ["xxp_foo1", "xxp_foo2"]),
    ("foo.+$", project2, ["xxp_foo1", "xxp_foo2"]),
    ("foo.*$", project2, ["xxp_foo1", "xxp_foo2", "unrelated_xxp_foo"]),
]


@pytest.mark.parametrize(
    "experiments, local_project, expected",
    [
        *REGEXES_REGULAR_PROJECT,
        *REGEXES_BARE_PROJECT,
    ],
)
def test_list_experiments_regex_local_context(experiments, expected, runs_with_attributes, local_project):
    result = list_experiments(experiments, context=local_project)
    assert result == expected


@pytest.mark.parametrize(
    "experiments, local_project, expected",
    REGEXES_REGULAR_PROJECT,
)
def test_list_experiments_regex_global_context1(
    experiments, expected, local_project, runs_with_attributes, global_context_project1
):
    result = list_experiments(experiments)
    assert result == expected


@pytest.mark.parametrize(
    "experiments, local_project, expected",
    REGEXES_BARE_PROJECT,
)
def test_list_experiments_regex_global_context2(
    experiments, expected, local_project, runs_with_attributes, global_context_project2
):
    result = list_experiments(experiments)
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
