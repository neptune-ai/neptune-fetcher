import os
from datetime import (
    datetime,
    timezone,
)

import pandas as pd
import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize(
    "runs_filter, attributes_filter, expected_attributes",
    [
        (
            r"^linear_history_root$",
            r".*-value$",
            {
                "run": ["linear_history_root"],
                "int-value:int": [1],
                "float-value:float": [1.0],
                "str-value:string": ["hello_1"],
                "bool-value:bool": [False],
                "datetime-value:datetime": [datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc)],
            },
        ),
        (
            r"^linear_history_root$",
            [],
            {
                "run": ["linear_history_root"],
            },
        ),
        (
            "^non_exist$",
            "^foo0$",
            {
                "run": [],
            },
        ),
        (
            r"^linear_history_root$",
            r"^foo.*$",
            {
                "run": ["linear_history_root"],
                "foo0:float_series": [0.1 * 9],
                "foo1:float_series": [0.2 * 9],
            },
        ),
        (
            r"^linear_history_root$",
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root"],
                "foo0:float_series": [0.1 * 9],
            },
        ),
        (
            "^linear_history_root$",
            AttributeFilter(name="foo0$") | AttributeFilter(name=".*-value$"),
            {
                "run": ["linear_history_root"],
                "int-value:int": [1],
                "float-value:float": [1.0],
                "str-value:string": ["hello_1"],
                "bool-value:bool": [False],
                "datetime-value:datetime": [datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc)],
                "foo0:float_series": [0.1 * 9],
            },
        ),
        (
            r"^linear_history_root$|^linear_history_fork2$",
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root", "linear_history_fork2"],
                "foo0:float_series": [0.1 * 9, 0.7 * 19],
            },
        ),
        (
            ["linear_history_root", "linear_history_fork2"],
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root", "linear_history_fork2"],
                "foo0:float_series": [0.1 * 9, 0.7 * 19],
            },
        ),
        (
            r"forked_history_root|forked_history_fork1",
            r".*-value$",
            {
                "run": ["forked_history_root", "forked_history_fork1"],
                "int-value:int": [1, 2],
                "float-value:float": [1.0, 2.0],
                "str-value:string": ["hello_1", "hello_2"],
                "bool-value:bool": [False, True],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.matches("sys/custom_run_id", r"forked_history_root|forked_history_fork1"),
            r".*-value$",
            {
                "run": ["forked_history_root", "forked_history_fork1"],
                "int-value:int": [1, 2],
                "float-value:float": [1.0, 2.0],
                "str-value:string": ["hello_1", "hello_2"],
                "bool-value:bool": [False, True],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.eq("sys/name", "exp_with_linear_history"),
            # matches runs with experiment_name 'exp_with_linear_history'
            r".*-value$",
            {
                "run": ["linear_history_fork1", "linear_history_fork2", "linear_history_root"],
                "int-value:int": [2, 3, 1],
                "float-value:float": [2.0, 3.0, 1.0],
                "str-value:string": ["hello_2", "hello_3", "hello_1"],
                "bool-value:bool": [True, False, False],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.exists(Attribute("str-value", type="string")),  # matches runs that have config 'str-value'
            r".*-value$",
            {
                "run": [
                    "forked_history_fork1",
                    "forked_history_fork2",
                    "forked_history_root",
                    "linear_history_fork1",
                    "linear_history_fork2",
                    "linear_history_root",
                ],
                "int-value:int": [2, 3, 1, 2, 3, 1],
                "float-value:float": [2.0, 3.0, 1.0, 2.0, 3.0, 1.0],
                "str-value:string": ["hello_2", "hello_3", "hello_1", "hello_2", "hello_3", "hello_1"],
                "bool-value:bool": [True, False, False, True, False, False],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                ],
            },
        ),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_fetch_runs_table(
    new_project_id,
    runs_filter,
    attributes_filter,
    expected_attributes,
    type_suffix_in_column_names: bool,
):
    df = runs.fetch_runs_table(
        project=new_project_id,
        runs=runs_filter,
        attributes=attributes_filter,
        sort_by=Attribute("sys/custom_run_id", type="string"),
        sort_direction="desc",
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    expected_data = {
        trim_suffix(k, type_suffix_in_column_names): v
        for k, v in sorted(expected_attributes.items(), key=lambda x: (x[0], "") if isinstance(x[0], str) else x[0])
    }
    expected = pd.DataFrame(expected_data).sort_values("run", ascending=False)
    expected["run"] = expected["run"].astype(object)
    expected.set_index("run", drop=True, inplace=True)

    pd.testing.assert_frame_equal(df, expected)


def trim_suffix(name, type_suffix_in_column_names):
    if type_suffix_in_column_names:
        return name
    else:
        if isinstance(name, tuple):
            return name[0].split(":")[0], name[1]
        else:
            return name.split(":")[0]
