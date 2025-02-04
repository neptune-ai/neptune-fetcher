#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import (
    Any,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.internal.attribute import (
    AttributeDefinition,
    AttributeValue,
)
from neptune_fetcher.alpha.internal.exception import ConflictingAttributeTypes
from neptune_fetcher.alpha.internal.identifiers import SysName
from neptune_fetcher.alpha.internal.types import FloatSeriesAggregations


def convert_experiment_table_to_dataframe(
    experiment_data: dict[SysName, list[AttributeValue]],
    selected_aggregations: dict[AttributeDefinition, set[str]],
    type_suffix_in_column_names: bool,
) -> pd.DataFrame:
    index_column_name = "experiment"

    if not experiment_data:
        return pd.DataFrame(index=[index_column_name])

    def convert_row(values: list[AttributeValue]) -> dict[tuple[str, str], Any]:
        row = {}
        for value in values:
            column_name = get_column_name(value)
            if column_name in row:
                raise ConflictingAttributeTypes([value.attribute_definition.name])
            if value.attribute_definition.type == "float_series":
                float_series_aggregations: FloatSeriesAggregations = value.value
                selected_subset = selected_aggregations.get(value.attribute_definition, set())
                agg_subset_values = get_aggregation_subset(float_series_aggregations, selected_subset)
                for agg_name, agg_value in agg_subset_values.items():
                    row[(column_name, agg_name)] = agg_value
            else:
                row[(column_name, "")] = value.value
        return row

    def get_column_name(attr: AttributeValue) -> str:
        return f"{attr.attribute_definition.name}:{attr.attribute_definition.type}"

    def get_aggregation_subset(
        float_series_aggregations: FloatSeriesAggregations, selected_subset: set[str]
    ) -> dict[str, Any]:
        result = {}
        for agg_name in ("last", "min", "max", "average", "variance"):
            if agg_name in selected_subset:
                result[agg_name] = getattr(float_series_aggregations, agg_name)
        return result

    def transform_column_names(df: pd.DataFrame) -> pd.DataFrame:
        if type_suffix_in_column_names:
            return df

        # Transform the column by removing the type
        original_columns = df.columns
        df.columns = [
            (col[0].rsplit(":", 1)[0], col[1]) if isinstance(col, tuple) else col.rsplit(":", 1)[0]
            for col in df.columns
        ]

        # Check for duplicate names
        duplicated_names = df.columns[df.columns.duplicated(keep=False)]  # type: ignore
        duplicated_names_set = set(duplicated_names)
        if duplicated_names.any():
            conflicting_types: dict[str, set[str]] = {}
            for original_col, new_col in zip(original_columns, df.columns):
                if isinstance(new_col, str):
                    continue

                if new_col in duplicated_names_set:
                    conflicting_types.setdefault(new_col[0], set()).add(original_col[0].rsplit(":", 1)[1])

            raise ConflictingAttributeTypes(conflicting_types.keys())  # TODO: add conflicting types to the exception

        return df

    rows: list[dict[Union[str, tuple[str, str]], Any]] = []
    for sys_name, values in experiment_data.items():
        row: dict[Union[str, tuple[str, str]], Any] = convert_row(values)  # type: ignore
        row[index_column_name] = sys_name
        rows.append(row)

    dataframe = pd.DataFrame(rows)
    dataframe = transform_column_names(dataframe)
    dataframe.set_index(index_column_name, drop=True, inplace=True)
    dataframe.columns = pd.MultiIndex.from_tuples(dataframe.columns, names=["attribute", "aggregation"])

    return dataframe
