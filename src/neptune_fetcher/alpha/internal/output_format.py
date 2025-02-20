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
    Generator,
    Iterable,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd

from neptune_fetcher.alpha.exceptions import ConflictingAttributeTypes
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.attribute_types import FloatSeriesAggregations
from neptune_fetcher.alpha.internal.retrieval.attribute_values import AttributeValue
from neptune_fetcher.alpha.internal.retrieval.metrics import (
    AttributePathIndex,
    ExperimentNameIndex,
    FloatPointValue,
    IsPreviewIndex,
    PreviewCompletionIndex,
    StepIndex,
    TimestampIndex,
    ValueIndex,
)

__all__ = (
    "convert_table_to_dataframe",
    "create_dataframe",
)


def convert_table_to_dataframe(
    table_data: dict[str, list[AttributeValue]],
    selected_aggregations: dict[AttributeDefinition, set[str]],
    type_suffix_in_column_names: bool,
    index_column_name: str = "experiment",
) -> pd.DataFrame:

    if not table_data:
        return pd.DataFrame(
            index=pd.Index([], name=index_column_name),
            columns=pd.MultiIndex.from_tuples([], names=["attribute", "aggregation"]),
        )

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
    for label, values in table_data.items():
        row: dict[Union[str, tuple[str, str]], Any] = convert_row(values)  # type: ignore
        row[index_column_name] = label
        rows.append(row)

    dataframe = pd.DataFrame(rows)
    dataframe = transform_column_names(dataframe)
    dataframe.set_index(index_column_name, drop=True, inplace=True)
    dataframe.columns = pd.MultiIndex.from_tuples(dataframe.columns, names=["attribute", "aggregation"])

    sorted_columns = sorted(dataframe.columns, key=lambda x: (x[0], x[1]))
    dataframe = dataframe[sorted_columns]

    return dataframe


def create_dataframe(
    data_points: Iterable[FloatPointValue],
    *,
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    index_column_name: str,
    timestamp_column_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Creates a memory-efficient DataFrame directly from FloatPointValue tuples.

    Note that `data_points` must be sorted by (experiment name, path) to ensure correct
    categorical codes.

    There is an intermediate processing step where we represent paths as categorical codes
    Example:

    Assuming there are 2 user columns called "foo" and "bar", 2 steps each. The intermediate
    DF will have this shape:

            experiment  path   step  value
        0     exp-name     0    1.0    0.0
        1     exp-name     0    2.0    0.5
        1     exp-name     1    1.0    1.5
        1     exp-name     1    2.0    2.5

    `path_mapping` would contain {"foo": 0, "bar": 1}. The column names will be restored before returning the
    DF, which then can be sorted based on its columns.

    The reason for the intermediate representation is that logging a metric called eg. "step" would conflict
    with our "step" column during df.reset_index(), and we would crash.
    Operating on integer codes is safe, as they can never appear as valid metric names.

    If `timestamp_column_name` is provided, timestamp will be included in the DataFrame under the
    specified column.
    """

    path_mapping: dict[str, int] = {}
    experiment_mapping: dict[str, int] = {}

    def generate_categorized_rows(float_point_values: Iterable[FloatPointValue]) -> Generator[Tuple, None, None]:
        last_experiment_name, last_experiment_category = None, None
        last_path_name, last_path_category = None, None

        for point in float_point_values:
            exp_category = (
                last_experiment_category
                if last_experiment_name == point[ExperimentNameIndex]
                else experiment_mapping.get(point[ExperimentNameIndex], None)  # type: ignore
            )
            path_category = (
                last_path_category
                if last_path_name == point[AttributePathIndex]
                else path_mapping.get(point[AttributePathIndex], None)  # type: ignore
            )

            if exp_category is None:
                exp_category = len(experiment_mapping)
                experiment_mapping[point[ExperimentNameIndex]] = exp_category  # type: ignore
                last_experiment_name, last_experiment_category = point[ExperimentNameIndex], exp_category
            if path_category is None:
                path_category = len(path_mapping)
                path_mapping[point[AttributePathIndex]] = path_category  # type: ignore
                last_path_name, last_path_category = point[AttributePathIndex], path_category

            # Only include columns that we know we need. Note that the order of must match the
            # the `types` list below.
            head = (
                exp_category,
                path_category,
                point[StepIndex],
                point[ValueIndex],
            )
            if include_point_previews and timestamp_column_name:
                tail: Tuple[Any, ...] = (point[TimestampIndex], point[IsPreviewIndex], point[PreviewCompletionIndex])
            elif timestamp_column_name:
                tail = (point[TimestampIndex],)
            elif include_point_previews:
                tail = (point[IsPreviewIndex], point[PreviewCompletionIndex])
            else:
                tail = ()

            yield head + tail

    types = [
        (index_column_name, "uint32"),
        ("path", "uint32"),
        ("step", "float64"),
        ("value", "float64"),
    ]

    if timestamp_column_name:
        types.append((timestamp_column_name, "uint64"))

    if include_point_previews:
        types.append(("is_preview", "bool"))
        types.append(("preview_completion", "float64"))

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(data_points), dtype=types),
    )

    experiment_dtype = pd.CategoricalDtype(categories=list(experiment_mapping.keys()))
    df[index_column_name] = pd.Categorical.from_codes(df[index_column_name], dtype=experiment_dtype)

    df = _pivot_and_reindex_df(df, include_point_previews, index_column_name, timestamp_column_name)
    df = _restore_path_column_names(df, path_mapping, type_suffix_in_column_names)

    # MultiIndex DFs need to have column index order swapped: value/metric_name -> metric_name/value.
    # We also sort columns, but only after the original names have been restored.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns.names = (None, None)
        df = df.swaplevel(axis=1)
        df = df.sort_index(axis=1, level=0)
    else:
        df.columns.name = None
        df = df.sort_index(axis=1)

    return df


def _pivot_and_reindex_df(
    df: pd.DataFrame,
    include_point_previews: bool,
    index_column_name: str = "experiment",
    timestamp_column_name: Optional[str] = None,
) -> pd.DataFrame:
    values: Union[str, list[str]] = "value"

    # Create column multi-index if necessary, otherwise we stick to a flat "value" column
    if include_point_previews or timestamp_column_name:
        values = ["value"]
        if timestamp_column_name:
            df[timestamp_column_name] = pd.to_datetime(df[timestamp_column_name], unit="ms", origin="unix", utc=True)
            values.append(timestamp_column_name)
        if include_point_previews:
            values.append("is_preview")
            values.append("preview_completion")

    df = df.pivot(index=[index_column_name, "step"], columns="path", values=values)
    df = df.reset_index()
    df[index_column_name] = df[index_column_name].astype(str)
    df = df.sort_values(by=[index_column_name, "step"], ignore_index=True)
    df = df.set_index([index_column_name, "step"])

    return df


def _restore_path_column_names(
    df: pd.DataFrame, path_mapping: dict[str, int], type_suffix_in_column_names: bool
) -> pd.DataFrame:
    """
    Accepts an DF in an intermediate format in _create_dataframe, and the mapping of column names.
    Restores colum names in the DF based on the mapping.
    """

    # We need to reverse the mapping to index -> column name
    if type_suffix_in_column_names:
        reverse_mapping = {index: path + ":float_series" for path, index in path_mapping.items()}
    else:
        reverse_mapping = {index: path for path, index in path_mapping.items()}
    return df.rename(columns=reverse_mapping)
