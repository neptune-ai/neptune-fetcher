import os
import statistics
from datetime import datetime

import pandas as pd
import pytest

from neptune_fetcher.v1 import (
    fetch_experiments_table,
    list_experiments,
)
from neptune_fetcher.v1.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.data import (
    FLOAT_SERIES_PATHS,
    PATH,
    STRING_SERIES_PATHS,
    TEST_DATA,
    TEST_DATA_VERSION,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test__fetch_experiments_table(project, run_with_attributes, sort_direction):
    df = fetch_experiments_table(
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
        project=project.project_identifier,
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
    "arg_experiments",
    [
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        [TEST_DATA.exp_name(0), TEST_DATA.exp_name(1), TEST_DATA.exp_name(2)],
        Filter.name_in(TEST_DATA.exp_name(0), TEST_DATA.exp_name(1), TEST_DATA.exp_name(2)),
        (
            Filter.name_eq(TEST_DATA.exp_name(0))
            | Filter.name_eq(TEST_DATA.exp_name(1))
            | Filter.name_eq(TEST_DATA.exp_name(2))
        ),
    ],
)
@pytest.mark.parametrize(
    "arg_attributes",
    [
        f"{PATH}/int-value|{PATH}/float-value|{PATH}/metrics/step",
        [f"{PATH}/int-value", f"{PATH}/float-value", f"{PATH}/metrics/step"],
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
    project,
    run_with_attributes,
    arg_experiments,
    arg_attributes,
    type_suffix_in_column_names,
):
    df = fetch_experiments_table(
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        experiments=arg_experiments,
        attributes=arg_attributes,
        type_suffix_in_column_names=type_suffix_in_column_names,
        project=project.project_identifier,
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
        project=project.project_identifier,
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
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
        AttributeFilter(f"{PATH}/metrics/string-series-value_0", type_in=["string_series"], aggregations=["last"])
        | AttributeFilter(f"{PATH}/metrics/string-series-value_1", type_in=["string_series"])
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_string_series(
    project, run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:2]]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":string_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:2]],
            (f"{PATH}/metrics/string-series-value_0" + suffix, "last"): [
                TEST_DATA.experiments[i].string_series[f"{PATH}/metrics/string-series-value_0"][-1] for i in range(2)
            ],
            (f"{PATH}/metrics/string-series-value_1" + suffix, "last"): [
                TEST_DATA.experiments[i].string_series[f"{PATH}/metrics/string-series-value_1"][-1] for i in range(2)
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert df.shape == expected.shape
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(f"{PATH}/metrics/histogram-series-value_0", type_in=["histogram_series"], aggregations=["last"])
        | AttributeFilter(f"{PATH}/metrics/histogram-series-value_1", type_in=["histogram_series"])
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_histogram_series(
    project, run_with_attributes, attr_filter, type_suffix_in_column_names
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:2]]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    suffix = ":histogram_series" if type_suffix_in_column_names else ""
    expected = pd.DataFrame(
        {
            "experiment": [exp.name for exp in TEST_DATA.experiments[:2]],
            (f"{PATH}/metrics/histogram-series-value_0" + suffix, "last"): [
                TEST_DATA.experiments[i].fetcher_histogram_series()[f"{PATH}/metrics/histogram-series-value_0"][-1]
                for i in range(2)
            ],
            (f"{PATH}/metrics/histogram-series-value_1" + suffix, "last"): [
                TEST_DATA.experiments[i].fetcher_histogram_series()[f"{PATH}/metrics/histogram-series-value_1"][-1]
                for i in range(2)
            ],
        }
    ).set_index("experiment", drop=True)
    expected.columns = pd.MultiIndex.from_tuples(expected.columns, names=["attribute", "aggregation"])
    assert df.shape == expected.shape
    pd.testing.assert_frame_equal(df[expected.columns], expected)
    assert df[expected.columns].columns.equals(expected.columns)


@pytest.mark.parametrize(
    "attr_filter",
    [
        AttributeFilter(f"{PATH}/metrics/string-series-value_0", type_in=["string_series"], aggregations=[]),
        AttributeFilter(f"{PATH}/metrics/string-series-value_0", type_in=["string_series"], aggregations=["min"]),
    ],
)
def test__fetch_experiments_table_with_attributes_filter_for_series_wrong_aggregation(
    project, run_with_attributes, attr_filter
):
    df = fetch_experiments_table(
        project=project.project_identifier,
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:2]]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=True,
    )
    assert df.empty


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
        project=project.project_identifier,
        experiments=Filter.name_in(*[exp.name for exp in TEST_DATA.experiments[:3]]),
        sort_by=Attribute("sys/name", type="string"),
        sort_direction="asc",
        attributes=attr_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
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


@pytest.mark.parametrize(
    "arg_experiments, expected_subset",
    [
        (None, TEST_DATA.experiment_names),
        (".*", TEST_DATA.experiment_names),
        ("", TEST_DATA.experiment_names),
        ("test_alpha", TEST_DATA.experiment_names),
        (Filter.matches_all(Attribute("sys/name", type="string"), ".*"), TEST_DATA.experiment_names),
    ],
)
def test_list_experiments_with_regex_and_filters_matching_all(project, arg_experiments, expected_subset):
    """We need to check if expected names are a subset of all names returned, as
    the test data could contain other experiments"""
    names = list_experiments(
        project=project.project_identifier,
        experiments=arg_experiments,
    )
    assert set(expected_subset) <= set(names)


@pytest.mark.parametrize(
    "regex, expected",
    [
        (f"alpha_1.*{TEST_DATA_VERSION}", [f"test_alpha_1_{TEST_DATA_VERSION}"]),
        (
            f"alpha_[2,3].*{TEST_DATA_VERSION}",
            [f"test_alpha_2_{TEST_DATA_VERSION}", f"test_alpha_3_{TEST_DATA_VERSION}"],
        ),
        ("not-found", []),
        ("experiment_999", []),
    ],
)
def test_list_experiments_with_regex_matching_some(project, regex, expected):
    """This check is more strict than test_list_experiments_with_regex_matching_all, as we are able
    to predict the exact output because of the filtering applied"""
    names = list_experiments(
        project=project.project_identifier,
        experiments=regex,
    )
    assert len(names) == len(expected)
    assert set(names) == set(expected)


@pytest.mark.parametrize(
    "arg_experiments, expected",
    [
        (Filter.eq(Attribute("sys/name", type="string"), ""), []),
        (Filter.name_in(*TEST_DATA.experiment_names), TEST_DATA.experiment_names),
        (
            Filter.matches_all(Attribute("sys/name", type="string"), [f"alpha.*{TEST_DATA_VERSION}", "_3"]),
            [f"test_alpha_3" f"_{TEST_DATA_VERSION}"],
        ),
        (TEST_DATA.experiment_names, TEST_DATA.experiment_names),
        (
            Filter.matches_none(Attribute("sys/name", type="string"), ["alpha_3", "alpha_4", "alpha_5"])
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [
                f"test_alpha_0_{TEST_DATA_VERSION}",
                f"test_alpha_1_{TEST_DATA_VERSION}",
                f"test_alpha_2_{TEST_DATA_VERSION}",
            ],
        ),
        (Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_123"), []),
        (
            Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_1_{TEST_DATA_VERSION}"],
        ),
        (
            (
                Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
                | Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_2")
            )
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_1_{TEST_DATA_VERSION}", f"test_alpha_2_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.ne(Attribute(f"{PATH}/str-value", type="string"), "hello_1")
            & Filter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello_2")
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_2_{TEST_DATA_VERSION}"],
        ),
        (Filter.eq(Attribute(f"{PATH}/int-value", type="int"), 12345), []),
        (
            Filter.eq(Attribute(f"{PATH}/int-value", type="int"), 2)
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_2_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.eq(Attribute(f"{PATH}/int-value", type="int"), 2)
            | Filter.eq(Attribute(f"{PATH}/int-value", type="int"), 3)
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_2_{TEST_DATA_VERSION}", f"test_alpha_3_{TEST_DATA_VERSION}"],
        ),
        (Filter.eq(Attribute(f"{PATH}/float-value", type="float"), 1.2345), []),
        (
            Filter.eq(Attribute(f"{PATH}/float-value", type="float"), 3)
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_3_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.eq(Attribute(f"{PATH}/bool-value", type="bool"), False)
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [
                f"test_alpha_1_{TEST_DATA_VERSION}",
                f"test_alpha_3_{TEST_DATA_VERSION}",
                f"test_alpha_5_{TEST_DATA_VERSION}",
            ],
        ),
        (
            Filter.eq(Attribute(f"{PATH}/bool-value", type="bool"), True)
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [
                f"test_alpha_0_{TEST_DATA_VERSION}",
                f"test_alpha_2_{TEST_DATA_VERSION}",
                f"test_alpha_4_{TEST_DATA_VERSION}",
            ],
        ),
        (Filter.gt(Attribute(f"{PATH}/datetime-value", type="datetime"), datetime.now()), []),
        (
            Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "no-such-string"),
            [],
        ),
        (
            Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), ["string-1-0", "string-1-1"])
            & Filter.matches_all(Attribute("sys/name", type="string"), TEST_DATA_VERSION),
            [f"test_alpha_1_{TEST_DATA_VERSION}"],
        ),
        (
            (
                Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-1-0")
                | Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0")
            )
            & Filter.matches_all(Attribute("sys/name", type="string"), TEST_DATA_VERSION),
            [f"test_alpha_0_{TEST_DATA_VERSION}", f"test_alpha_1_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.contains_none(
                Attribute(f"{PATH}/string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [
                f"test_alpha_0_{TEST_DATA_VERSION}",
                f"test_alpha_4_{TEST_DATA_VERSION}",
                f"test_alpha_5_{TEST_DATA_VERSION}",
            ],
        ),
        (
            Filter.contains_none(
                Attribute(f"{PATH}/string_set-value", type="string_set"),
                ["string-1-0", "string-2-0", "string-3-0"],
            )
            & Filter.contains_all(Attribute(f"{PATH}/string_set-value", type="string_set"), "string-0-0")
            & Filter.matches_all(Attribute("sys/name", type="string"), f"test_alpha_[0-9]_{TEST_DATA_VERSION}"),
            [f"test_alpha_0_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.eq(Attribute("sys/name", type="string"), f"test_alpha_0_{TEST_DATA_VERSION}"),
            [f"test_alpha_0_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.exists(Attribute(f"{PATH}/files/file-value.txt", type="file")),
            [f"test_alpha_0_{TEST_DATA_VERSION}"],
        ),
        (
            Filter.exists(Attribute(FLOAT_SERIES_PATHS[0], type="float_series")),
            TEST_DATA.experiment_names,
        ),
        (
            Filter.exists(Attribute(STRING_SERIES_PATHS[0], type="string_series")),
            TEST_DATA.experiment_names,
        ),
        # ( # todo: histogram_series not supported yet in the nql
        #     Filter.exists(Attribute(HISTOGRAM_SERIES_PATHS[0], type="histogram_series")),
        #     TEST_DATA.experiment_names,
        # ),
    ],
)
def test_list_experiments_with_filter_matching_some(project, arg_experiments, expected):
    names = list_experiments(
        project=project.project_identifier,
        experiments=arg_experiments,
    )
    assert set(names) == set(expected)
    assert len(names) == len(expected)
