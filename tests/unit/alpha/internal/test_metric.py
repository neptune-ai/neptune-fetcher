import pytest
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from neptune_fetcher.alpha.internal.metric import _ResultAccumulator, _FloatPointValues

@pytest.fixture
def accumulator():
    return _ResultAccumulator()

def test_add_float_point_values(accumulator):
    # Create a sample _FloatPointValues object
    float_point_values = _FloatPointValues(
        experiment="exp1",
        path="path1",
        steps=[1.0, 2.0],
        values=[10.0, 20.0],
        timestamps=[datetime(2023, 1, 1), datetime(2023, 1, 2)]
    )

    # Add the float point values to the accumulator
    accumulator.add_float_point_values(float_point_values)

    # Check if the vectors are updated correctly
    assert len(accumulator.timestamp_vectors) == 1
    assert len(accumulator.step_vectors) == 1
    assert len(accumulator.value_vectors) == 1

    # Check if the mappings are updated correctly
    assert "exp1" in accumulator.experiment_name_to_id
    assert "path1" in accumulator.path_name_to_id

def test_create_dataframe(accumulator):
    # Add first set of float point values
    float_point_values_1 = _FloatPointValues(
        experiment="exp1",
        path="path1",
        steps=[1.0, 2.0],
        values=[10.0, 20.0],
        timestamps=[datetime(2023, 1, 1), datetime(2023, 1, 2)]
    )
    accumulator.add_float_point_values(float_point_values_1)

    # Add second set of float point values
    float_point_values_2 = _FloatPointValues(
        experiment="exp2",
        path="path2",
        steps=[3.0, 4.0],
        values=[30.0, 40.0],
        timestamps=[datetime(2023, 1, 3), datetime(2023, 1, 4)]
    )
    accumulator.add_float_point_values(float_point_values_2)

    # Create a DataFrame using a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        df = accumulator.create_dataframe(executor)

    # Check if the DataFrame is created correctly
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # Total number of entries

    # Check the contents of the DataFrame
    assert df['experiment'].tolist() == ['exp1', 'exp1', 'exp2', 'exp2']
    assert df['path'].tolist() == ['path1', 'path1', 'path2', 'path2']
    assert df['step'].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert df['value'].tolist() == [10.0, 20.0, 30.0, 40.0]
    assert df['timestamp'].tolist() == [
        datetime(2023, 1, 1), datetime(2023, 1, 2),
        datetime(2023, 1, 3), datetime(2023, 1, 4)
    ]

def test_empty_dataframe(accumulator):
    # Create a DataFrame using a ThreadPoolExecutor without adding any data
    with ThreadPoolExecutor(max_workers=5) as executor:
        df = accumulator.create_dataframe(executor)

    # Check if the DataFrame is created correctly
    assert isinstance(df, pd.DataFrame)
    assert df.empty  # The DataFrame should be empty
    assert len(df) == 0  # No rows should be present

    # Check that all expected columns are present
    expected_columns = ['experiment', 'path', 'timestamp', 'step', 'value']
    assert list(df.columns) == expected_columns
    
    
