import os
import random
import statistics
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

import pandas as pd
import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import fetch_experiments_table
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)


@dataclass
class ExperimentData:
    name: str
    config: dict[str, any]
    float_series: dict[str, list[float]]
    unique_series: dict[str, list[float]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


FLOAT_SERIES_PATHS = [f"metrics/float-series-value_{j}" for j in range(5)]


@dataclass
class TestData:
    neptune_project: str = os.getenv("NEPTUNE_E2E_PROJECT")
    experiments: list[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            for i in range(6):
                experiment_name = f"fetch_experiments_table_{i}_{uuid.uuid4()}"
                config = {
                    "test/int-value": 10,
                    "test/float-value": 0.5,
                    "test/str-value": "hello",
                    "test/bool-value": True,
                    "test/datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                }

                float_series = {
                    path: [float(step**2) + float(random.uniform(0, 1)) for step in range(10)]
                    for path in FLOAT_SERIES_PATHS
                }
                unique_series = {
                    f"metrics/unique-series-{i}-{j}": [float(random.uniform(0, 100)) for _ in range(10)]
                    for j in range(3)
                }

                float_series["metrics/step"] = [float(step) for step in range(10)]
                self.experiments.append(
                    ExperimentData(
                        name=experiment_name, config=config, float_series=float_series, unique_series=unique_series
                    )
                )


TEST_DATA = TestData()
NOW = datetime.now(timezone.utc)


@pytest.fixture(scope="module")
def run_with_attributes(project):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        run = Run(
            project=project_id,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        run.log_configs(experiment.config)

        for step in range(len(experiment.float_series["metrics/step"])):
            metrics_data = {path: values[step] for path, values in experiment.float_series.items()}
            metrics_data["metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        runs[experiment.name] = run

    for run in runs.values():
        run.close()

    return runs


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test__fetch_experiments_table(sort_direction, run_with_attributes):
    df = fetch_experiments_table(
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
    )

    experiments = [experiment.name for experiment in TEST_DATA.experiments]
    expected = pd.DataFrame(
        {
            "experiment": experiments if sort_direction == "asc" else experiments[::-1],
        }
    )
    assert len(df) == 6
    assert pd.testing.assert_frame_equal(df, expected)


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
        r"test/int-value|test/float-value|metrics/step",
        AttributeFilter.any(
            AttributeFilter("test/int-value", type_in=["int"]),
            AttributeFilter("test/float-value", type_in=["float"]),
            AttributeFilter("metrics/step", type_in=["float_series"]),
        ),
        AttributeFilter("test/int-value", type_in=["int"])
        | AttributeFilter("test/float-value", type_in=["float"])
        | AttributeFilter("metrics/step", type_in=["float_series"]),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test__fetch_experiments_table_with_attributes_filter(
    run_with_attributes, attr_filter, experiment_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        experiments=experiment_filter,
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    string_suffix = ":string" if type_suffix_in_column_names else ""
    float_suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            "test/int-value" + string_suffix: [10 for _ in range(3)],
            "test/float-value" + string_suffix: [0.5 for _ in range(3)],
            "metrics/step"
            + float_suffix
            + ":last": [TEST_DATA.experiments[i].float_series["metrics/step"][-1] for i in range(3)],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(
            "metrics/step", type_in=["float_series"], aggregations=["last", "min", "max", "average", "variance"]
        )
        | AttributeFilter(FLOAT_SERIES_PATHS[0], type_in=["float_series"], aggregations=["average", "variance"])
        | AttributeFilter(FLOAT_SERIES_PATHS[1], type_in=["float_series"]),
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_metrics(
    run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            ("metrics/step" + suffix, "last"): [
                TEST_DATA.experiments[i].float_series["metrics/step"][-1] for i in range(3)
            ],
            ("metrics/step" + suffix, "min"): [
                TEST_DATA.experiments[i].float_series["metrics/step"][0] for i in range(3)
            ],
            ("metrics/step" + suffix, "max"): [
                TEST_DATA.experiments[i].float_series["metrics/step"][-1] for i in range(3)
            ],
            ("metrics/step" + suffix, "average"): [
                statistics.fmean(TEST_DATA.experiments[i].float_series["metrics/step"]) for i in range(3)
            ],
            ("metrics/step" + suffix, "variance"): [
                statistics.variance(TEST_DATA.experiments[i].float_series["metrics/step"]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "average"): [
                statistics.fmean(TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "variance"): [
                statistics.variance(TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]]) for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[1]][-1] for i in range(3)
            ],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)
    assert df.columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(name_matches_all=f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}"),
        AttributeFilter(
            name_matches_all=f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
            name_matches_none=".*[5-9].*",
        ),
        f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
    ],
)
def test__fetch_experiments_table_with_attributes_regex_filter_for_metrics(
    run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        experiments=ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:3]],
            ("metrics/step" + suffix, "last"): [
                TEST_DATA.experiments[i].float_series["metrics/step"][-1] for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[0] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[0]][-1] for i in range(3)
            ],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [
                TEST_DATA.experiments[i].float_series[FLOAT_SERIES_PATHS[1]][-1] for i in range(3)
            ],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)
    assert df.columns.equals(expected.columns)
