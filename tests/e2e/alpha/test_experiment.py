import os
import random
import statistics
import time
import uuid
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)

import pandas as pd
import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import (
    Context,
    fetch_experiments_table,
    list_experiments,
)
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
)
from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-02-01"
PATH = f"test/test-experiment-{TEST_DATA_VERSION}"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]


@dataclass
class ExperimentData:
    name: str
    config: dict[str, any]
    string_sets: dict[str, list[str]]
    float_series: dict[str, list[float]]
    unique_series: dict[str, list[float]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class TestData:
    experiments: list[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            random.seed(TEST_DATA_VERSION)
            for i in range(6):
                experiment_name = f"test_experiment_{i}_{TEST_DATA_VERSION}"
                config = {
                    f"{PATH}/int-value": i,
                    f"{PATH}/float-value": float(i),
                    f"{PATH}/str-value": f"hello_{i}",
                    f"{PATH}/bool-value": i % 2 == 0,
                    f"{PATH}/datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                }

                string_sets = {f"{PATH}/string_set-value": [f"string-{i}-{j}" for j in range(5)]}
                float_series = {
                    path: [float(step**2) + float(random.uniform(0, 1)) for step in range(10)]
                    for path in FLOAT_SERIES_PATHS
                }
                unique_series = {
                    f"{PATH}/metrics/unique-series-{i}-{j}": [float(random.uniform(0, 100)) for _ in range(10)]
                    for j in range(3)
                }

                float_series[f"{PATH}/metrics/step"] = [float(step) for step in range(10)]
                self.experiments.append(
                    ExperimentData(
                        name=experiment_name,
                        config=config,
                        string_sets=string_sets,
                        float_series=float_series,
                        unique_series=unique_series,
                    )
                )

    @property
    def experiment_names(self):
        return [exp.name for exp in self.experiments]


TEST_DATA = TestData()


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(project, client):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_id),
                ExperimentFilter.name_in(experiment.name),
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
            run.log_metrics(data=metrics_data, step=step, timestamp=datetime(2025, 1, 31, 0, 0, int(step)))

        runs[experiment.name] = run
    for run in runs.values():
        run.close()

    # Make sure all experiments are visible in the system before starting tests
    for _ in range(15):
        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project.project_identifier),
                ExperimentFilter.name_in(*TEST_DATA.experiment_names),
            )
        )

        if len(existing.items) == len(TEST_DATA.experiment_names):
            return runs

        time.sleep(1)

    raise RuntimeError("Experiments did not appear in the system in time")


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test__fetch_experiments_table(project, run_with_attributes, sort_direction):
    df = fetch_experiments_table(
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
        context=_context(project),
    )

    experiments = [experiment.name for experiment in TEST_DATA.experiments]
    expected = pd.DataFrame(
        {
            "experiment": experiments if sort_direction == "asc" else experiments[::-1],
            ("sys/name", ""): experiments if sort_direction == "asc" else experiments[::-1],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert len(df) == 6
    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "experiment_filter",
    [
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        ExperimentFilter.name_in(TEST_DATA.exp_name(0), TEST_DATA.exp_name(1), TEST_DATA.exp_name(2)),
        ExperimentFilter.name_eq(TEST_DATA.exp_name(0))
        | ExperimentFilter.name_eq(TEST_DATA.exp_name(1))
        | ExperimentFilter.name_eq(TEST_DATA.exp_name(2)),
    ],
)
@pytest.mark.parametrize(
    "attr_filter",
    [
        f"{PATH}/int-value|{PATH}/float-value|{PATH}/metrics/step",
        AttributeFilter.any(
            AttributeFilter(f"{PATH}/int-value", type_in=["int"]),
            AttributeFilter(f"{PATH}/float-value", type_in=["float"]),
            AttributeFilter(f"{PATH}/metrics/step", type_in=["float_series"]),
        ),
        AttributeFilter(f"{PATH}/int-value", type_in=["int"])
        | AttributeFilter(f"{PATH}/float-value", type_in=["float"])
        | AttributeFilter(f"{PATH}/metrics/step", type_in=["float_series"]),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test__fetch_experiments_table_with_attributes_filter(
    project, run_with_attributes, attr_filter, experiment_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        experiments=experiment_filter,
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=_context(project),
    )

    def suffix(name):
        return f":{name}" if type_suffix_in_column_names else ""

    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            (f"{PATH}/int-value{suffix('int')}", ""): [i for i in range(3)],
            (f"{PATH}/float-value{suffix('float')}", ""): [float(i) for i in range(3)],
            (f"{PATH}/metrics/step{suffix('float_series')}", "last"): [
                TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"][-1] for i in range(3)
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df[expected.columns], expected)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(
            f"{PATH}/metrics/step", type_in=["float_series"], aggregations=["last", "min", "max", "average", "variance"]
        )
        | AttributeFilter(FLOAT_SERIES_PATHS[0], type_in=["float_series"], aggregations=["average", "variance"])
        | AttributeFilter(FLOAT_SERIES_PATHS[1], type_in=["float_series"]),
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_metrics(
    project, run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=_context(project),
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            (f"{PATH}/metrics/step" + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"][-1] for i in range(3)
            ],
            (f"{PATH}/metrics/step" + suffix, "min"): [
                TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"][0] for i in range(3)
            ],
            (f"{PATH}/metrics/step" + suffix, "max"): [
                TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"][-1] for i in range(3)
            ],
            (f"{PATH}/metrics/step" + suffix, "average"): [
                statistics.fmean(TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"]) for i in range(3)
            ],
            (f"{PATH}/metrics/step" + suffix, "variance"): [
                statistics.pvariance(TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "average"): [
                statistics.fmean(TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "variance"): [
                statistics.pvariance(TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[1]][-1] for i in range(3)
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert df.shape == (3, 8)
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(name_matches_all=f"{PATH}/metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}"),
        AttributeFilter(
            name_matches_all=f"{PATH}/metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
            name_matches_none=".*value_[5-9].*",
        ),
        f"{PATH}/metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
    ],
)
def test__fetch_experiments_table_with_attributes_regex_filter_for_metrics(
    project, run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=_context(project),
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            (f"{PATH}/metrics/step" + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[f"{PATH}/metrics/step"][-1] for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]][-1] for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[1]][-1] for i in range(3)
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


def _context(project):
    return Context(project=project.project_identifier, api_token=env.NEPTUNE_API_TOKEN.get())


@pytest.mark.parametrize(
    "regex, expected_subset",
    [
        (None, TEST_DATA.experiment_names),
        (".*", TEST_DATA.experiment_names),
        ("", TEST_DATA.experiment_names),
        ("test_experiment", TEST_DATA.experiment_names),
        (ExperimentFilter.matches_all(Attribute("sys/name", type="string"), ".*"), TEST_DATA.experiment_names),
    ],
)
def test_list_experiments_with_regex_and_filters_matching_all(project, regex, expected_subset):
    """We need to check if expected names are a subset of all names returned, as
    the test data could contain other experiments"""
    names = list_experiments(regex, context=_context(project))
    assert set(expected_subset) <= set(names)


@pytest.mark.parametrize(
    "regex, expected",
    [
        (f"experiment_1.*{TEST_DATA_VERSION}", [f"test_experiment_1_{TEST_DATA_VERSION}"]),
        (
            f"experiment_[2,3].*{TEST_DATA_VERSION}",
            [f"test_experiment_2_{TEST_DATA_VERSION}", f"test_experiment_3_{TEST_DATA_VERSION}"],
        ),
        ("not-found", []),
        ("experiment_999", []),
    ],
)
def test_list_experiments_with_regex_matching_some(project, regex, expected):
    """This check is more strict than test_list_experiments_with_regex_matching_all, as we are able
    to predict the exact output because of the filtering applied"""
    names = list_experiments(regex, context=_context(project))
    assert len(names) == len(expected)
    assert set(names) == set(expected)


@pytest.mark.parametrize(
    "filter_, expected",
    [
        (ExperimentFilter.eq(Attribute("sys/name", type="string"), ""), []),
        (ExperimentFilter.name_in(*TEST_DATA.experiment_names), TEST_DATA.experiment_names),
        (
            ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), [f"experiment.*{TEST_DATA_VERSION}", "_3"]
            ),
            [f"test_experiment_3" f"_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.matches_none(
                Attribute("sys/name", type="string"), ["experiment_3", "experiment_4", "experiment_5"]
            )
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [
                f"test_experiment_0_{TEST_DATA_VERSION}",
                f"test_experiment_1_{TEST_DATA_VERSION}",
                f"test_experiment_2_{TEST_DATA_VERSION}",
            ],
        ),
        (ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_123"), []),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_1_{TEST_DATA_VERSION}"],
        ),
        (
            (
                ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
                | ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_2")
            )
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_1_{TEST_DATA_VERSION}", f"test_experiment_2_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.ne(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
            & ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_2")
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_2_{TEST_DATA_VERSION}"],
        ),
        (ExperimentFilter.eq(Attribute(f"{PATH}/int-value", type="int"), 12345), []),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/int-value", type="int"), 2)
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_2_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/int-value", type="int"), 2)
            | ExperimentFilter.eq(Attribute(f"{PATH}/int-value", type="int"), 3)
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_2_{TEST_DATA_VERSION}", f"test_experiment_3_{TEST_DATA_VERSION}"],
        ),
        (ExperimentFilter.eq(Attribute(f"{PATH}/float-value", type="float"), 1.2345), []),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/float-value", type="float"), 3)
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_3_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/bool-value", type="bool"), False)
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [
                f"test_experiment_1_{TEST_DATA_VERSION}",
                f"test_experiment_3_{TEST_DATA_VERSION}",
                f"test_experiment_5_{TEST_DATA_VERSION}",
            ],
        ),
        (
            ExperimentFilter.eq(Attribute(f"{PATH}/bool-value", type="bool"), True)
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [
                f"test_experiment_0_{TEST_DATA_VERSION}",
                f"test_experiment_2_{TEST_DATA_VERSION}",
                f"test_experiment_4_{TEST_DATA_VERSION}",
            ],
        ),
        # TODO: add tests for datetime once we fix how Attribute handles the value
        # (ExperimentFilter.gt(Attribute(f"{PATH}/datetime-value", type="datetime"), datetime.now()), []),
        (
            ExperimentFilter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "no-such-string"),
            [],
        ),
        (
            ExperimentFilter.contains_all(
                Attribute(f"{PATH}/string_set-value", type="string_set"), ["string-1-0", "string-1-1"]
            )
            & ExperimentFilter.matches_all(Attribute("sys/name", type="string"), TEST_DATA_VERSION),
            [f"test_experiment_1_{TEST_DATA_VERSION}"],
        ),
        (
            (
                ExperimentFilter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-1-0")
                | ExperimentFilter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0")
            )
            & ExperimentFilter.matches_all(Attribute("sys/name", type="string"), TEST_DATA_VERSION),
            [f"test_experiment_0_{TEST_DATA_VERSION}", f"test_experiment_1_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.contains_none(
                Attribute(f"{PATH}/string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [
                f"test_experiment_0_{TEST_DATA_VERSION}",
                f"test_experiment_4_{TEST_DATA_VERSION}",
                f"test_experiment_5_{TEST_DATA_VERSION}",
            ],
        ),
        (
            ExperimentFilter.contains_none(
                Attribute(f"{PATH}/string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & ExperimentFilter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0")
            & ExperimentFilter.matches_all(
                Attribute("sys/name", type="string"), f"test_experiment_[0-9]_{TEST_DATA_VERSION}"
            ),
            [f"test_experiment_0_{TEST_DATA_VERSION}"],
        ),
        (
            ExperimentFilter.eq(Attribute("sys/name", type="string"), f"test_experiment_0_{TEST_DATA_VERSION}"),
            [f"test_experiment_0_{TEST_DATA_VERSION}"],
        ),
    ],
)
def test_list_experiments_with_filter_matching_some(project, filter_, expected):
    names = list_experiments(filter_, context=_context(project))
    assert set(names) == set(expected)
    assert len(names) == len(expected)
