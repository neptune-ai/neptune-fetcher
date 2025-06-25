import itertools as it
import os
from datetime import timedelta
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pytest

from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.identifiers import (
    AttributeDefinition,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_fetcher.internal.output_format import create_series_dataframe
from neptune_fetcher.internal.retrieval.series import SeriesValue
from neptune_fetcher.v1 import fetch_series
from neptune_fetcher.v1.filters import (
    AttributeFilter,
    Filter,
)
from tests.e2e.data import (
    NOW,
    NUMBER_OF_STEPS,
    TEST_DATA,
    ExperimentData,
)

NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")


def create_expected_data(
    experiments: list[ExperimentData],
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    series_data: dict[RunAttributeDefinition, list[SeriesValue]] = {}
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
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(path)
                    filtered_exps.add(experiment.name)
                    filtered.append(
                        SeriesValue(
                            step,
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
@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name_matches_all=[r".*/metrics/.*"], type_in=["string_series"]),
        ".*/metrics/.*",
        AttributeFilter(name_matches_all=[r".*/metrics/.*"], type_in=["string_series"])
        | AttributeFilter(name_matches_all=[".*/int-value"]),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        [exp.name for exp in TEST_DATA.experiments[:3]],
    ],
)
@pytest.mark.parametrize("include_time", [None, "absolute"])
def test__fetch_series(
    project,
    type_suffix_in_column_names,
    step_range,
    tail_limit,
    include_time,
    arg_experiments,
    arg_attributes,
):
    experiments = TEST_DATA.experiments[:3]

    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data(experiments, include_time, step_range, tail_limit)

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps
