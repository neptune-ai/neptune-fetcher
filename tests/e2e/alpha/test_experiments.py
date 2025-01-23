import os
import statistics
import uuid
from datetime import (
    datetime,
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

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAMES = [f"fetch_experiments_table_{i}_{uuid.uuid4()}" for i in range(6)]
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
DATETIME_VALUE2 = datetime(2025, 2, 1, 0, 0, 0, 0, timezone.utc)

FLOAT_SERIES_PATHS = [f"metrics/float-series-value_{i}" for i in range(5)]

FLOAT_SERIES_STEPS = [step for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(project):
    runs = {}
    for experiment_name in EXPERIMENT_NAMES:
        project_id = project.project_identifier
        run_id = str(uuid.uuid4())

        run = Run(
            project=project_id,
            run_id=run_id,
            experiment_name=experiment_name,
        )

        data = {
            "test/int-value": 10,
            "test/float-value": 0.5,
            "test/str-value": "hello",
            "test/bool-value": True,
            "test/datetime-value": DATETIME_VALUE,
        }
        run.log_configs(data)

        for step, value in zip(FLOAT_SERIES_STEPS, FLOAT_SERIES_VALUES):
            run.log_metrics(data={path: value for path in FLOAT_SERIES_PATHS}, step=step)
            run.log_metrics(data={"metrics/step": step}, step=step)
        runs[experiment_name] = run

    for run in runs.values():
        run.close()

    return runs


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test__fetch_experiments_table(sort_direction):
    df = fetch_experiments_table(sort_by=Attribute("sys/name", type="string"), sort_direction=sort_direction)

    experiments = EXPERIMENT_NAMES
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
        f"{EXPERIMENT_NAMES[0]}|{EXPERIMENT_NAMES[1]}|{EXPERIMENT_NAMES[2]}",
        ExperimentFilter.name_in(EXPERIMENT_NAMES[0], EXPERIMENT_NAMES[1], EXPERIMENT_NAMES[2]),
        ExperimentFilter.name_eq(EXPERIMENT_NAMES[0])
        | ExperimentFilter.name_eq(EXPERIMENT_NAMES[1])
        | ExperimentFilter.name_eq(EXPERIMENT_NAMES[2]),
    ],
)
@pytest.mark.parametrize(
    "attr_filter",
    [
        r"test/int-value|test/float-value|metrics/step",
        AttributeFilter.any(
            AttributeFilter("test/int-value", type="int"),
            AttributeFilter("test/float-value", type="float"),
            AttributeFilter("metrics/step", type="float_series"),
        ),
        AttributeFilter("test/int-value", type="int")
        | AttributeFilter("test/float-value", type="float")
        | AttributeFilter("metrics/step", type="float_series"),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test__fetch_experiments_table_with_attributes_filter(attr_filter, experiment_filter, type_suffix_in_column_names):
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
            "experiment": EXPERIMENT_NAMES[:3],
            "test/int-value" + string_suffix: [10 for _ in range(3)],
            "test/float-value" + string_suffix: [0.5 for _ in range(3)],
            "metrics/step" + float_suffix + ":last": [FLOAT_SERIES_STEPS[-1] for _ in range(3)],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(
            "matrics/step", type_in=["float_series"], aggregations=["last", "min", "max", "average", "variance"]
        )
        | AttributeFilter(FLOAT_SERIES_PATHS[0], type_in=["float_series"], aggregations=["average", "variance"])
        | AttributeFilter(FLOAT_SERIES_PATHS[1], type_in=["float_series"]),
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_metrics(attr_filter, type_suffix_in_column_names):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        experiments=ExperimentFilter.name_in(EXPERIMENT_NAMES[0:3]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES[0:3],
            ("metrics/step" + suffix, "last"): [FLOAT_SERIES_STEPS[-1] for _ in range(3)],
            ("metrics/step" + suffix, "min"): [FLOAT_SERIES_STEPS[0] for _ in range(3)],
            ("metrics/step" + suffix, "max"): [FLOAT_SERIES_STEPS[-1] for _ in range(3)],
            ("metrics/step" + suffix, "average"): [statistics.fmean(FLOAT_SERIES_STEPS) for _ in range(3)],
            ("metrics/step" + suffix, "variance"): [statistics.variance(FLOAT_SERIES_STEPS) for _ in range(3)],
            (FLOAT_SERIES_PATHS[0] + suffix, "average"): [statistics.fmean(FLOAT_SERIES_VALUES) for _ in range(3)],
            (FLOAT_SERIES_PATHS[0] + suffix, "variance"): [statistics.fmean(FLOAT_SERIES_VALUES) for _ in range(3)],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [FLOAT_SERIES_VALUES[-1] for _ in range(3)],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)
    assert df.columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter.name_matches_all(f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}"),
        AttributeFilter(
            name_matches_all=f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
            name_matches_none=".*[5-9].*",
        ),
        f"metrics/step|{FLOAT_SERIES_PATHS[0]}|{FLOAT_SERIES_PATHS[1]}",
    ],
)
def test__fetch_experiments_table_with_attributes_regex_filter_for_metrics(attr_filter, type_suffix_in_column_names):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        experiments=ExperimentFilter.name_in(EXPERIMENT_NAMES[0:3]),
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":float_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": EXPERIMENT_NAMES[0:3],
            ("metrics/step" + suffix, "last"): [FLOAT_SERIES_STEPS[-1] for _ in range(3)],
            (FLOAT_SERIES_PATHS[0] + suffix, "last"): [FLOAT_SERIES_VALUES[-1] for _ in range(3)],
            (FLOAT_SERIES_PATHS[1] + suffix, "last"): [FLOAT_SERIES_VALUES[-1] for _ in range(3)],
        }
    )
    assert df.shape[0] == 3
    assert pd.testing.assert_frame_equal(df, expected)
    assert df.columns.equals(expected.columns)
