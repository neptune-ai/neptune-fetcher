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

from neptune_fetcher.alpha import fetch_series
from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.context import get_context
from neptune_fetcher.alpha.internal.identifiers import (
    RunIdentifier,
    SysId,
)
from neptune_fetcher.alpha.internal.output_format import create_series_dataframe
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs
from neptune_fetcher.alpha.internal.retrieval.series import (
    RunAttributeDefinition,
    StringSeriesValue,
)


@dataclass
class ExperimentData:
    name: str
    string_series: Dict[str, List[str]]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


PATH = "metrics/fetch-series"
STRING_SERIES_PATHS = [f"{PATH}/metrics/string-series-value_{j}" for j in range(2)]
NUMBER_OF_STEPS = 10
NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")

TEST_DATA_VERSION = "v4"


@dataclass
class TestData:
    experiments: List[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        if not self.experiments:
            for i in range(6):
                experiment_name = f"pye2e-alpha-fetch-series_{i}_{TEST_DATA_VERSION}"

                string_series = {
                    path: [f"string-{step * (j + i + 1)}" for step in range(NUMBER_OF_STEPS)]
                    for j, path in enumerate(STRING_SERIES_PATHS)
                }

                self.experiments.append(ExperimentData(name=experiment_name, string_series=string_series))


NOW = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
TEST_DATA = TestData()


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(client, project):
    expected_experiments = {exp.name for exp in TEST_DATA.experiments}
    existing_experiments = it.chain.from_iterable(
        [
            p.items
            for p in fetch_experiment_sys_attrs(
                client=client,
                project_identifier=identifiers.ProjectIdentifier(project.project_identifier),
                filter_=Filter.name_in(*expected_experiments),
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

        for step in range(NUMBER_OF_STEPS):
            series_data = {path: value[step] for path, value in experiment.string_series.items()}
            run.log_string_series(data=series_data, step=step + 1, timestamp=NOW + timedelta(seconds=int(step)))
            # TODO: fix step + 1 -> step when backend returns step starting from 0

        runs[experiment.name] = run

    for run in runs.values():
        run.close()

    return runs


def create_expected_data(
    experiments: list[ExperimentData],
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    series_data: dict[RunAttributeDefinition, list[StringSeriesValue]] = {}
    sys_id_label_mapping: dict[SysId, str] = {}

    columns = set()
    filtered_exps = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -np.inf,
        step_range[1] if step_range[1] is not None else np.inf,
    )
    for experiment in experiments:
        steps = range(NUMBER_OF_STEPS)
        sys_id_label_mapping[SysId(experiment.run_id)] = experiment.name

        for path, series in experiment.string_series.items():
            run_attr = RunAttributeDefinition(
                RunIdentifier(identifiers.ProjectIdentifier(NEPTUNE_PROJECT), SysId(experiment.run_id)),
                AttributeDefinition(path, type="string_series"),
            )

            filtered = []
            for step in steps:
                step_val = step + 1  # TODO: fix step+1 -> step when backend returns step starting from 0
                if step_filter[0] <= step_val <= step_filter[1]:
                    columns.add(path)
                    filtered_exps.add(experiment.name)
                    filtered.append(
                        StringSeriesValue(
                            step_val,
                            series[step],
                            int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000,
                        )
                    )
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            series_data.setdefault(run_attr, []).extend(limited)

    df = create_series_dataframe(
        series_data,
        sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "absolute_time"), (c, "value")] for c in sorted_columns]
        return df, list(it.chain.from_iterable(absolute_columns)), filtered_exps
    else:
        return df, sorted_columns, filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("attr_filter", [AttributeFilter(name_matches_all=[r".*"], type_in=["string_series"]), ".*"])
@pytest.mark.parametrize(
    "exp_filter",
    [
        lambda: Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        lambda: f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
    ],
)
@pytest.mark.parametrize("include_time", [None, "absolute"])
def test__fetch_series(
    project, type_suffix_in_column_names, step_range, tail_limit, include_time, attr_filter, exp_filter
):
    experiments = TEST_DATA.experiments[:3]

    result = fetch_series(
        experiments=exp_filter(),
        attributes=attr_filter,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        context=get_context().with_project(project.project_identifier),
    )

    expected, columns, filtered_exps = create_expected_data(experiments, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps
