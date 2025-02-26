import itertools
import os
import random
import uuid
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Iterable

import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import (
    list_attributes,
    set_project,
)
from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "v4"
PATH = f"test/test-attribute-{TEST_DATA_VERSION}"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]
STRING_SERIES_PATHS = [f"{PATH}/metrics/string-series-value_{j}" for j in range(2)]


@dataclass
class ExperimentData:
    name: str
    config: dict[str, any]
    string_sets: dict[str, list[str]]
    float_series: dict[str, list[float]]
    unique_series: dict[str, list[float]]
    string_series: dict[str, list[str]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def all_attribute_names(self) -> set[str]:
        return set(
            itertools.chain(
                self.config.keys(),
                self.string_sets.keys(),
                self.float_series.keys(),
                self.unique_series.keys(),
            )
        )


@dataclass
class TestData:
    experiments: list[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            for i in range(6):
                experiment_name = f"test_alpha_attribute_{i}_{TEST_DATA_VERSION}"
                config = {
                    f"{PATH}/int-value": i,
                    f"{PATH}/float-value": float(i),
                    f"{PATH}/str-value": f"hello_{i}",
                    f"{PATH}/bool-value": i % 2 == 0,
                    f"{PATH}/datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                    f"{PATH}/unique-value-{i}": f"unique_{i}",
                }

                string_sets = {f"{PATH}/string_set-value": [f"string-{i}-{j}" for j in range(5)]}
                float_series = {
                    path: [float(step**2) + float(random.uniform(0, 1)) for step in range(10)]
                    for path in FLOAT_SERIES_PATHS
                }

                float_series[f"{PATH}/metrics/step"] = [float(step) for step in range(10)]

                string_series = {path: [f"string-{i}-{j}" for j in range(10)] for path in STRING_SERIES_PATHS}

                self.experiments.append(
                    ExperimentData(
                        name=experiment_name,
                        config=config,
                        string_sets=string_sets,
                        float_series=float_series,
                        string_series=string_series,
                        unique_series={},
                    )
                )

    @property
    def experiment_names(self):
        return [exp.name for exp in self.experiments]

    @property
    def all_attribute_names(self):
        return set(itertools.chain.from_iterable(exp.all_attribute_names for exp in self.experiments))


TEST_DATA = TestData()
NOW = datetime.now(timezone.utc)


@pytest.fixture(autouse=True)
def context(project):
    set_project(project.project_identifier)


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(project, client):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_id),
                Filter.name_in(experiment.name),
            )
        )
        if existing.items:
            continue

        run = Run(
            project=project_id,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        run.log_configs(experiment.config)
        # This is how neptune-scale allows setting string set values currently
        run.log(tags_add=experiment.string_sets)

        for step in range(len(experiment.float_series[f"{PATH}/metrics/step"])):
            metrics_data = {path: values[step] for path, values in experiment.float_series.items()}
            metrics_data[f"{PATH}/metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

            series_data = {path: values[step] for path, values in experiment.string_series.items()}
            run.log_string_series(data=series_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        runs[experiment.name] = run
    for run in runs.values():
        run.close()

    return runs


def _drop_sys_attr_names(attributes: Iterable[str]) -> list[str]:
    return [attr for attr in attributes if not attr.startswith("sys/")]


# Convenience filter to limit searches to experiments belonging to this test,
# in case the run has some extra experiments.
EXPERIMENTS_IN_THIS_TEST = Filter.name_in(*TEST_DATA.experiment_names)


@pytest.mark.parametrize(
    "experiment_filter",
    (
        EXPERIMENTS_IN_THIS_TEST,
        rf"test_alpha_attribute_[0-9]+_{TEST_DATA_VERSION}",
        ".*",
        None,
    ),
)
@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (PATH, TEST_DATA.all_attribute_names),
        (f"{PATH}/int-value", {f"{PATH}/int-value"}),
        (rf"{PATH}/metrics/.*", FLOAT_SERIES_PATHS + [f"{PATH}/metrics/step"] + STRING_SERIES_PATHS),
        (
            rf"{PATH}/.*-value$",
            {
                f"{PATH}/int-value",
                f"{PATH}/float-value",
                f"{PATH}/str-value",
                f"{PATH}/bool-value",
                f"{PATH}/datetime-value",
                f"{PATH}/string_set-value",
            },
        ),
        (rf"{PATH}/unique-value-[0-9]", {f"{PATH}/unique-value-{i}" for i in range(6)}),
        (AttributeFilter(name_matches_all=PATH), TEST_DATA.all_attribute_names),
        (AttributeFilter(name_eq=f"{PATH}/float-value"), {f"{PATH}/float-value"}),
        (
            AttributeFilter.any(AttributeFilter(name_matches_all="^(foo)"), AttributeFilter(name_matches_all=PATH)),
            TEST_DATA.all_attribute_names,
        ),
        (AttributeFilter(name_matches_none=".*"), []),
    ],
)
def test_list_attributes_known_in_all_experiments_with_name_filter_excluding_sys(
    attribute_filter, expected, experiment_filter
):
    attributes = _drop_sys_attr_names(list_attributes(attributes=attribute_filter, experiments=experiment_filter))
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "name_filter",
    (
        None,
        "",
        ".*",
        AttributeFilter(name_matches_all=".*"),
        AttributeFilter(),
    ),
)
def test_list_attributes_all_names_from_all_experiments_excluding_sys(name_filter):
    attributes = _drop_sys_attr_names(list_attributes(experiments=EXPERIMENTS_IN_THIS_TEST, attributes=name_filter))
    assert set(attributes) == set(TEST_DATA.all_attribute_names)
    assert len(attributes) == len(TEST_DATA.all_attribute_names)


@pytest.mark.parametrize(
    "filter_",
    (
        "unknown",
        ".*unknown.*",
        "sys/abcdef",
        " ",
        AttributeFilter(name_eq=".*"),
        AttributeFilter(name_matches_all="unknown"),
    ),
)
def test_list_attributes_unknown_name(filter_):
    attributes = list_attributes(attributes=filter_)
    assert not attributes


@pytest.mark.parametrize(
    "attribute_filter, experiment_filter, expected",
    [
        (r"unique-value-[0-2]", EXPERIMENTS_IN_THIS_TEST, {f"{PATH}/unique-value-{i}" for i in range(3)}),
        (
            rf"{PATH}/unique-value-[0-2]",
            f"test_alpha_attribute_.*_{TEST_DATA_VERSION}",
            {f"{PATH}/unique-value-{i}" for i in range(3)},
        ),
        (
            rf"{PATH}/unique-value-.*",
            rf"test_alpha_attribute_(0|2)_{TEST_DATA_VERSION}",
            {f"{PATH}/unique-value-0", f"{PATH}/unique-value-2"},
        ),
        (
            rf"{PATH}/unique-value-.*",
            Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0"),
            {f"{PATH}/unique-value-0"},
        ),
        (
            rf"{PATH}/unique-value-.*",
            Filter.contains_none(
                Attribute(f"{PATH}/string_set-value", type="string_set"), ["string-0-0", "string-1-0", "string-4-0"]
            ),
            {f"{PATH}/unique-value-{i}" for i in (2, 3, 5)},
        ),
        (
            AttributeFilter(name_matches_none="sys/.*", name_matches_all=".*"),
            Filter.gt(Attribute(f"{PATH}/int-value", type="int"), 1234) & EXPERIMENTS_IN_THIS_TEST,
            [],
        ),
        (
            AttributeFilter(name_matches_none="sys/.*", name_matches_all=".*"),
            Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_12345") & EXPERIMENTS_IN_THIS_TEST,
            [],
        ),
        (
            f"{PATH}/unique-value",
            Filter.lt(Attribute(f"{PATH}/int-value", type="int"), 3) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in range(3)},
        ),
        (
            f"{PATH}/unique-value",
            Filter.eq(Attribute(f"{PATH}/bool-value", type="bool"), False) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in (1, 3, 5)},
        ),
        (
            f"{PATH}/unique-value",
            Filter.eq(Attribute(f"{PATH}/bool-value", type="bool"), False) & EXPERIMENTS_IN_THIS_TEST,
            {f"{PATH}/unique-value-{i}" for i in (1, 3, 5)},
        ),
    ],
)
def test_list_attributes_depending_on_values_in_experiments(attribute_filter, experiment_filter, expected):
    attributes = list_attributes(attributes=attribute_filter, experiments=experiment_filter)
    assert set(attributes) == set(expected)
    assert len(attributes) == len(expected)


@pytest.mark.parametrize(
    "attribute_filter, expected",
    [
        (
            r"sys/(name|id)",
            {"sys/name", "sys/id"},
        ),
        (r"sys/.*id$", {"sys/custom_run_id", "sys/id"}),
        (AttributeFilter(name_matches_all=r"sys/(name|id)"), {"sys/name", "sys/id"}),
    ],
)
def test_list_attributes_sys_attrs(attribute_filter, expected):
    """A separate test for sys attributes, as we ignore them in tests above for simplicity."""

    attributes = list_attributes(attributes=attribute_filter)
    assert set(attributes) == expected
    assert len(attributes) == len(expected)
