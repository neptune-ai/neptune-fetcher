import itertools
import itertools as it
import os
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
from itertools import chain
from typing import (
    Dict,
    List,
    Literal,
    Union,
)

import numpy as np
import pandas as pd
import pytest
from neptune_scale import Run

from neptune_fetcher.alpha import set_project
from neptune_fetcher.alpha.experiments import fetch_metrics
from neptune_fetcher.alpha.filter import (
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs
from neptune_fetcher.alpha.internal.identifiers import ProjectIdentifier


@dataclass
class ExperimentData:
    name: str
    config: Dict[str, any]
    string_sets: Dict[str, List[str]]
    float_series: Dict[str, List[float]]
    unique_series: Dict[str, List[float]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


PATH = "alpha/e2e/tests"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]
NUMBER_OF_STEPS = 100
PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")

TEST_DATA_VERSION = "v4"


@dataclass
class TestData:
    experiments: List[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            for i in range(6):
                experiment_name = f"pye2e-alpha_{i}_{TEST_DATA_VERSION}"

                configs = {
                    f"{PATH}/test/int-value": i,
                    f"{PATH}/test/float-value": float(i),
                    f"{PATH}/test/str-value": f"hello_{i}",
                    f"{PATH}/test/bool-value": i % 2 == 0,
                    # The backend rounds the milliseconds component, so we're fine with just 0 to be more predictable
                    f"{PATH}/test/datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
                }

                string_sets = {f"{PATH}/test/string_set-value": [f"string-{i}-{x}" for x in range(5)]}

                float_series = {
                    path: [float(step * (j + i)) for step in range(NUMBER_OF_STEPS)]
                    for j, path in enumerate(FLOAT_SERIES_PATHS)
                }
                unique_series = {
                    f"{PATH}/metrics/unique-series-{i}-{j}": [
                        float((step + j**2) / (i + 1)) for step in range(NUMBER_OF_STEPS)
                    ]
                    for j in range(3)
                }

                float_series[f"{PATH}/metrics/step"] = [float(step) for step in range(NUMBER_OF_STEPS)]
                self.experiments.append(
                    ExperimentData(
                        name=experiment_name,
                        config=configs,
                        string_sets=string_sets,
                        float_series=float_series,
                        unique_series=unique_series,
                    )
                )

    @property
    def experiment_names(self):
        return [exp.name for exp in self.experiments]


NOW = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
TEST_DATA = TestData()


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(client):
    # TODO: remove this once we have a way to create experiments
    expected_experiments = {exp.name for exp in TEST_DATA.experiments}
    existing_experiments = it.chain.from_iterable(
        [
            p.items
            for p in fetch_experiment_sys_attrs(
                client=client,
                project_identifier=ProjectIdentifier(PROJECT),
                experiment_filter=ExperimentFilter.name_in(*expected_experiments),
            )
        ]
    )
    if expected_experiments == {exp.sys_name for exp in existing_experiments}:
        return {}

    runs = {}
    for experiment in TEST_DATA.experiments:
        run = Run(
            project=PROJECT,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        run.log_configs(experiment.config)
        # This is how neptune-scale allows setting string set values currently
        run.log(tags_add=experiment.string_sets)

        for index, step in enumerate(experiment.float_series[f"{PATH}/metrics/step"]):
            series = itertools.chain.from_iterable([experiment.float_series.items(), experiment.unique_series.items()])
            metrics_data = {path: values[index] for path, values in series}
            metrics_data[f"{PATH}/metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        runs[experiment.name] = run

    for run in runs.values():
        run.close()

    return runs


@pytest.fixture(autouse=True)
def set_default_context():
    set_project(PROJECT)


def create_expected_data(
    experiments: list[ExperimentData],
    type_suffix_in_column_names: bool,
    include_timestamp: Union[Literal["relative", "absolute"], None],
) -> pd.DataFrame:
    suffix = ":float_series" if type_suffix_in_column_names else ""
    rows = []
    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]

        for path, series in chain.from_iterable([experiment.float_series.items(), experiment.unique_series.items()]):
            for step in steps:
                rows.append(
                    (
                        experiment.name,
                        path + suffix,
                        NOW + timedelta(seconds=int(step)),
                        step,
                        series[int(step)],
                    )
                )

    df = pd.DataFrame(rows, columns=["experiment", "path", "timestamp", "step", "value"])
    if include_timestamp == "absolute":
        df = df.rename(columns={"timestamp": "absolute_time"})
        df = df.pivot(
            index=["experiment", "step"],
            columns="path",
            values=["value", "absolute_time"],
        )
        df = df.swaplevel(axis=1)
        df = df.sort_index(axis=1, level=0)
        df = df.reset_index()
        return df
    else:
        df = df.pivot(index=["experiment", "step"], columns="path", values="value")
        df = df.reset_index()
        df = df.sort_index(axis=1)
        df = df.rename_axis(None, axis=1)
        return df


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("step_range", [(0, 5), (0, None), (None, 5), (None, None)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("attr_filter", [AttributeFilter(name_matches_all=[r".*"], type_in=["float_series"]), ".*"])
@pytest.mark.parametrize(
    "exp_filter",
    [
        lambda: ExperimentFilter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        lambda: f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
    ],
)
@pytest.mark.parametrize("include_timestamp", [None, "absolute"])  # "relative",
def test__fetch_metrics_unique(
    type_suffix_in_column_names, step_range, tail_limit, include_timestamp, attr_filter, exp_filter
):
    experiments = TEST_DATA.experiments[:3]

    result = fetch_metrics(
        experiments=exp_filter(),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        step_range=step_range,
        tail_limit=tail_limit,
        include_timestamp=include_timestamp,
    )

    expected = create_expected_data(experiments, type_suffix_in_column_names, include_timestamp)

    if step_range != (None, None):
        step_min, step_max = step_range
        expected = expected[
            (expected["step"] >= (step_min if step_min is not None else -np.inf))
            & (expected["step"] <= (step_max if step_max is not None else np.inf))
        ].reset_index(drop=True)

    if tail_limit is not None:
        expected = expected.groupby("experiment").tail(tail_limit).reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)
