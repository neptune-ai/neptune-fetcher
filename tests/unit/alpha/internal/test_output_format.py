import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from neptune_fetcher.alpha.exceptions import ConflictingAttributeTypes
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.output_format import convert_experiment_table_to_dataframe
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.attribute_types import FloatSeriesAggregations
from neptune_fetcher.alpha.internal.retrieval.attribute_values import AttributeValue

EXPERIMENT_IDENTIFIER = identifiers.RunIdentifier(
    identifiers.ProjectIdentifier("project/abc"), identifiers.SysId("XXX-1")
)


def test_convert_experiment_table_to_dataframe_empty():
    # given
    experiment_data = {}

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data, selected_aggregations={}, type_suffix_in_column_names=False
    )

    # then
    assert dataframe.empty


def test_convert_experiment_table_to_dataframe_single_string():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data, selected_aggregations={}, type_suffix_in_column_names=False
    )

    # then
    assert dataframe.to_dict() == {
        ("attr1", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_string_with_type_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data, selected_aggregations={}, type_suffix_in_column_names=True
    )

    # then
    assert dataframe.to_dict() == {
        ("attr1:int", ""): {"exp1": 42},
    }


def test_convert_experiment_table_to_dataframe_single_float_series():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(
                AttributeDefinition("attr1", "float_series"),
                FloatSeriesAggregations(last=42.0, min=0.0, max=100, average=24.0, variance=100.0),
                EXPERIMENT_IDENTIFIER,
            ),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data,
        selected_aggregations={
            AttributeDefinition("attr1", "float_series"): {"last", "min", "variance"},
        },
        type_suffix_in_column_names=False,
    )

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
            AttributeValue(AttributeDefinition("attr1", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr2", "int"), 43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data, selected_aggregations={}, type_suffix_in_column_names=False
    )

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


def test_convert_experiment_table_to_dataframe_conflicting_types_with_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    dataframe = convert_experiment_table_to_dataframe(
        experiment_data, selected_aggregations={}, type_suffix_in_column_names=True
    )

    # then
    expected_data = pd.DataFrame.from_dict(
        {
            ("attr1/a:b:c:int", ""): {"exp1": 42.0, "exp2": float("nan")},
            ("attr1/a:b:c:float", ""): {"exp1": float("nan"), "exp2": 0.43},
        }
    )
    expected_data.index.name = "experiment"
    expected_data.columns = pd.MultiIndex.from_tuples(expected_data.columns, names=["attribute", "aggregation"])
    assert_frame_equal(dataframe, expected_data)


def test_convert_experiment_table_to_dataframe_conflicting_types_without_suffix():
    # given
    experiment_data = {
        identifiers.SysName("exp1"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "int"), 42, EXPERIMENT_IDENTIFIER),
        ],
        identifiers.SysName("exp2"): [
            AttributeValue(AttributeDefinition("attr1/a:b:c", "float"), 0.43, EXPERIMENT_IDENTIFIER),
        ],
    }

    # when
    with pytest.raises(ConflictingAttributeTypes) as exc_info:
        convert_experiment_table_to_dataframe(
            experiment_data, selected_aggregations={}, type_suffix_in_column_names=False
        )

    # then
    assert "attr1/a:b:c" in str(exc_info.value)
