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
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pytest
from neptune_scale import Run

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition.fetch_metrics import (
    _transform_with_absolute_timestamp,
    _transform_without_timestamp,
    fetch_metrics,
)
from neptune_fetcher.alpha.internal.context import get_context
from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs


@dataclass
class ExperimentData:
    name: str
    config: Dict[str, any]
    float_series: Dict[str, List[float]]
    unique_series: Dict[str, List[float]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


PATH = "metrics/fetch-metric"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]
NUMBER_OF_STEPS = 100
NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")

TEST_DATA_VERSION = "v2"


@dataclass
class TestData:
    experiments: List[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            for i in range(6):
                experiment_name = f"pye2e-alpha_{i}_{TEST_DATA_VERSION}"

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
                        name=experiment_name, config={}, float_series=float_series, unique_series=unique_series
                    )
                )


NOW = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
TEST_DATA = TestData()


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(client, project):
    # TODO: remove this once we have a way to create experiments
    expected_experiments = {exp.name for exp in TEST_DATA.experiments}
    existing_experiments = it.chain.from_iterable(
        [
            p.items
            for p in fetch_experiment_sys_attrs(
                client=client,
                project_identifier=identifiers.ProjectIdentifier(project.project_identifier),
                experiment_filter=Filter.name_in(*expected_experiments),
            )
        ]
    )
    if expected_experiments == {exp.sys_name for exp in existing_experiments}:
        return {}

    runs = {}
    for experiment in TEST_DATA.experiments:
        run = Run(
            project=project.project_identifier,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        for index, step in enumerate(experiment.float_series[f"{PATH}/metrics/step"]):
            series = itertools.chain.from_iterable([experiment.float_series.items(), experiment.unique_series.items()])
            metrics_data = {path: values[index] for path, values in series}
            metrics_data[f"{PATH}/metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        runs[experiment.name] = run

    for run in runs.values():
        run.close()

    return runs


def create_expected_data(
    experiments: list[ExperimentData],
    type_suffix_in_column_names: bool,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    rows = []
    columns = set()
    filtered_exps = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -np.inf,
        step_range[1] if step_range[1] is not None else np.inf,
    )
    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]

        for path, series in chain.from_iterable([experiment.float_series.items(), experiment.unique_series.items()]):
            filtered = []
            for step in steps:
                if step >= step_filter[0] and step <= step_filter[1]:
                    columns.add(f"{path}:float_series" if type_suffix_in_column_names else path)
                    filtered_exps.add(experiment.name)
                    filtered.append(
                        (
                            experiment.name,
                            path,
                            int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000,
                            step,
                            series[int(step)],
                        )
                    )
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered
            rows.extend(limited)

    df = pd.DataFrame(rows, columns=["experiment", "path", "timestamp", "step", "value"])
    df["experiment"] = df["experiment"].astype(str)
    df["path"] = df["path"].astype(str)
    df["timestamp"] = df["timestamp"].astype(int)
    df["step"] = df["step"].astype(float)
    df["value"] = df["value"].astype(float)

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "absolute_time"), (c, "value")] for c in sorted_columns]
        return (
            _transform_with_absolute_timestamp(df, type_suffix_in_column_names),
            list(chain.from_iterable(absolute_columns)),
            filtered_exps,
        )
    else:
        return _transform_without_timestamp(df, type_suffix_in_column_names), sorted_columns, filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("step_range", [(0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("attr_filter", [AttributeFilter(name_matches_all=[r".*"], type_in=["float_series"]), ".*"])
@pytest.mark.parametrize(
    "exp_filter",
    [
        lambda: Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        lambda: f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
    ],
)
@pytest.mark.parametrize("include_time", [None, "absolute"])  # "relative",
def test__fetch_metrics_unique(
    project, type_suffix_in_column_names, step_range, tail_limit, include_time, attr_filter, exp_filter
):
    experiments = TEST_DATA.experiments[:3]

    result = fetch_metrics(
        experiments=exp_filter(),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        step_range=step_range,
        tail_limit=tail_limit,
        include_time=include_time,
        context=get_context().with_project(project.project_identifier),
    )

    expected, columns, filtred_exps = create_expected_data(
        experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtred_exps
