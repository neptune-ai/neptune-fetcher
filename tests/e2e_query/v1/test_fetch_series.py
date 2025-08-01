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

from neptune_query import fetch_series
from neptune_query.filters import (
    AttributeFilter,
    Filter,
)
from neptune_query.internal import identifiers
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_series_dataframe
from neptune_query.internal.retrieval.series import SeriesValue
from tests.e2e_query.data import (
    FILE_SERIES_STEPS,
    NOW,
    NUMBER_OF_STEPS,
    TEST_DATA,
    ExperimentData,
)

NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")


def create_expected_data_string_series(
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
        "my-project",
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


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=r".*/metrics/.*", type=["string_series"]),
        ".*/metrics/string-series.*",
        AttributeFilter(name=r".*/metrics/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
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
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_string_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_string_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["string_series"]),
            True,
            None,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
            ".*/metrics/string-series.*",
            False,
            "absolute",
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
            True,
            None,
        ),
    ],
)
def test__fetch_string_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_string_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["string_series"]),
            (0.0, 5),
            None,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
            ".*/metrics/string-series.*",
            (0, None),
            3,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["string_series"]) | AttributeFilter(name=".*/int-value"),
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_string_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_string_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


def create_expected_data_histogram_series(
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

        for path, series in experiment.fetcher_histogram_series().items():
            run_attr = RunAttributeDefinition(
                RunIdentifier(identifiers.ProjectIdentifier(NEPTUNE_PROJECT), SysId(experiment.run_id)),
                AttributeDefinition(path, type="histogram_series"),
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
        "my-project",
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


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=".*/metrics/.*", type=["histogram_series"]),
        ".*/metrics/histogram-series.*",
        AttributeFilter(name=".*/metrics/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        [exp.name for exp in TEST_DATA.experiments[:3]],
    ],
)
@pytest.mark.parametrize(
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_histogram_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
            ".*/metrics/histogram-series.*",
            True,
            None,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=".*/metrics/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
            False,
            "absolute",
        ),
    ],
)
def test__fetch_histogram_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit",
    [
        (
            f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
            ".*/metrics/histogram-series.*",
            (0.0, 5),
            None,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=".*/metrics/.*", type=["histogram_series"]) | AttributeFilter(name=".*/int-value"),
            (0, None),
            3,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
            ".*/metrics/histogram-series.*",
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_histogram_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
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

    expected, columns, filtered_exps = create_expected_data_histogram_series(
        experiments, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


def create_expected_data_file_series(
    experiments: list[ExperimentData],
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
    project_identifier: str,
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
        steps = range(FILE_SERIES_STEPS)
        sys_id_label_mapping[SysId(experiment.run_id)] = experiment.name

        for path, series in experiment.file_series_matchers().items():
            run_attr = RunAttributeDefinition(
                RunIdentifier(identifiers.ProjectIdentifier(NEPTUNE_PROJECT), SysId(experiment.run_id)),
                AttributeDefinition(path, type="file_series"),
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
        project_identifier,
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


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=".*/files/.*", type=["file_series"]),
        ".*/files/file-series.*",
        AttributeFilter(name=".*/files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}",
        [exp.name for exp in TEST_DATA.experiments[:2]],
    ],
)
@pytest.mark.parametrize(
    "step_range, tail_limit, type_suffix_in_column_names, include_time",
    [
        ((0.0, 5), None, True, None),
        ((0, None), 3, False, "absolute"),
        ((None, 5), 5, True, None),
        ((None, None), None, False, "absolute"),
    ],
)
def test__fetch_file_series__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:1]

    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(
        experiments, include_time, step_range, tail_limit, project.project_identifier
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("step_range", [(0.0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)}",
            ".*/files/file-series.*",
            True,
            None,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:2]],
            AttributeFilter(name=".*/files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
            False,
            "absolute",
        ),
    ],
)
def test__fetch_file_series__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:1]

    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(
        experiments, include_time, step_range, tail_limit, project.project_identifier
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit",
    [
        (
            f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}",
            ".*/files/file-series.*",
            (0.0, 5),
            None,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:2]],
            AttributeFilter(name=".*/files/.*", type=["file_series"]) | AttributeFilter(name=".*/int-value"),
            (0, None),
            3,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)}",
            ".*/files/file-series.*",
            (None, 5),
            5,
        ),
    ],
)
def test__fetch_file_series__output_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:1]

    result = fetch_series(
        experiments=arg_experiments,
        attributes=arg_attributes,
        include_time=include_time,
        step_range=step_range,
        tail_limit=tail_limit,
        lineage_to_the_root=True,
        project=project.project_identifier,
    )

    expected, columns, filtered_exps = create_expected_data_file_series(
        experiments, include_time, step_range, tail_limit, project.project_identifier
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_exps
