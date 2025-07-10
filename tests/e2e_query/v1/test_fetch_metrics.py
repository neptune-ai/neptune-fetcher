import os
from datetime import timedelta
from itertools import chain
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from neptune_query import fetch_metrics
from neptune_query.filters import (
    AttributeFilter,
    Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.metrics import FloatPointValue
from tests.e2e_query.data import (
    NOW,
    PATH,
    TEST_DATA,
    ExperimentData,
)

NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")


def create_expected_data(
    project: str,
    experiments: list[ExperimentData],
    type_suffix_in_column_names: bool,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    metrics_data: dict[RunAttributeDefinition, list[FloatPointValue]] = {}
    sys_id_label_mapping: dict[SysId, str] = {SysId(experiment.name): experiment.name for experiment in experiments}

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
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(f"{path}:float_series" if type_suffix_in_column_names else path)
                    filtered_exps.add(experiment.name)
                    filtered.append(
                        (
                            int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000,
                            step,
                            series[int(step)],
                            False,
                            1.0,
                        )
                    )
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            attribute_run = RunAttributeDefinition(
                RunIdentifier(ProjectIdentifier(project), SysId(experiment.name)),
                AttributeDefinition(path, "float_series"),
            )
            metrics_data.setdefault(attribute_run, []).extend(limited)

    df = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        index_column_name="experiment",
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "absolute_time"), (c, "value")] for c in sorted_columns]
        return df, list(chain.from_iterable(absolute_columns)), filtered_exps
    else:
        return df, sorted_columns, filtered_exps


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ".*/metrics/.*",
        # Alternative should work too, see bug PY-137
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
        | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
        [exp.name for exp in TEST_DATA.experiments[:3]],
    ],
)
@pytest.mark.parametrize(
    "step_range,tail_limit,page_point_limit,type_suffix_in_column_names,include_time",
    [
        (
            (0, 5),
            None,
            50,
            True,
            None,
        ),
        (
            (0, None),
            3,
            1_000_000,
            False,
            "absolute",
        ),
        (
            (None, 5),
            5,
            50,
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_exps = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("page_point_limit", [50, 1_000_000])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            None,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",
            ".*/metrics/.*",
            False,
            "absolute",
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtred_exps = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtred_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])  # "relative",
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit,page_point_limit",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (0, 5),
            None,
            50,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",
            ".*/metrics/.*",
            (0, None),
            3,
            1_000_000,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (None, 5),
            5,
            50,
        ),
    ],
)
def test__fetch_metrics_unique__output_format_variants(
    project,
    arg_experiments,
    arg_attributes,
    type_suffix_in_column_names,
    include_time,
    step_range,
    tail_limit,
    page_point_limit,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtred_exps = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtred_exps
