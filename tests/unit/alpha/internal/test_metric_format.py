from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Generator

import pandas as pd
import pytest

from neptune_fetcher.alpha.internal.composition.fetch_metrics import _create_flat_dataframe
from neptune_fetcher.alpha.internal.retrieval.metrics import FloatPointValue

# Constants for the test
EXPERIMENTS = 5
PATHS = 5
STEPS = 10


def generate_float_point_values(
    experiments: int, paths: int, steps: int, preview: bool
) -> Generator[FloatPointValue, None, None]:
    for experiment in range(experiments):
        for path in range(paths):
            for step in range(steps):
                timestamp = datetime(2023, 1, 1, 0, 0, 0, 0, timezone.utc) + timedelta(seconds=step)
                yield (
                    f"exp{experiment}",
                    f"path{path}",
                    timestamp.timestamp(),
                    float(step),
                    float(step) * 100,
                    preview,
                    1.0 - (float(step) / 1000.0),
                )


@pytest.mark.parametrize("include_preview", [False, True])
def test_create_flat_dataframe(include_preview):
    float_point_values = list(generate_float_point_values(EXPERIMENTS, PATHS, STEPS, include_preview))

    """Test the creation of a flat DataFrame from float point values."""
    df = _create_flat_dataframe(float_point_values, include_point_previews=include_preview)

    # Check if the DataFrame is not empty
    assert not df.empty, "DataFrame should not be empty"

    # Check the shape of the DataFrame
    expected_rows = EXPERIMENTS * PATHS * STEPS
    assert df.shape[0] == expected_rows, f"DataFrame should have {expected_rows} rows"

    # Check the columns of the DataFrame
    expected_columns = ["experiment", "path", "timestamp", "step", "value"]
    if include_preview:
        expected_columns.extend(["is_preview", "preview_completion"])
    assert list(df.columns) == expected_columns, f"DataFrame should have columns {expected_columns}"

    # Check if the 'experiment' and 'path' columns are categorical
    assert pd.api.types.is_categorical_dtype(df["experiment"]), "'experiment' column should be categorical"
    assert pd.api.types.is_categorical_dtype(df["path"]), "'path' column should be categorical"

    # Convert DataFrame to list of tuples
    tuples_list = list(df.to_records(index=False))
    assert len(tuples_list) == expected_rows, "The list of tuples should have the same number of rows as the DataFrame"
