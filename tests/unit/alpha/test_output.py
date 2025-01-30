import pandas as pd
from pandas._testing import assert_frame_equal

from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.output import convert_experiment_table_to_dataframe
from neptune_fetcher.alpha.internal.types import (
    AttributeValue,
    FloatSeriesAggregatesSubset,
)

EXPERIMENT_IDENTIFIER = identifiers.ExperimentIdentifier(
    identifiers.ProjectIdentifier("project/abc"), identifiers.SysId("XXX-1")
)


def test_convert_experiment_table_to_dataframe_empty():
    # given
    experiment_data = {}

    # when
    dataframe = convert_experiment_table_to_dataframe(experiment_data, type_suffix_in_column_names=False)

    # then
    assert dataframe.empty


def test_convert_experiment_table_to_dataframe_single_string():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue("attr1", "int", 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(experiment_data, type_suffix_in_column_names=False)

    # then
    assert dataframe.to_dict() == {
        ("attr1", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_string_with_type_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue("attr1", "int", 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(experiment_data, type_suffix_in_column_names=True)

    # then
    assert dataframe.to_dict() == {
        ("attr1:int", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_float_series():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                "attr1",
                "float_series",
                FloatSeriesAggregatesSubset(last=42.0, min=0.0, variance=100.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(experiment_data, type_suffix_in_column_names=False)

    # then
    assert dataframe.to_dict() == {
        ("attr1", "last"): {"exp1": 42.0},
        ("attr1", "min"): {"exp1": 0.0},
        ("attr1", "variance"): {"exp1": 100.0},
    }


def test_convert_experiment_table_to_dataframe_disjoint_names():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue("attr1", "int", 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue("attr2", "int", 43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(experiment_data, type_suffix_in_column_names=False)

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            ("attr1", ""): {"exp1": 42.0, "exp2": float("nan")},
            ("attr2", ""): {"exp1": float("nan"), "exp2": 43.0},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns = pd.MultiIndex.from_tuples(expected_data.columns, names=["attribute", "aggregation"])
    assert_frame_equal(dataframe, expected_data)
