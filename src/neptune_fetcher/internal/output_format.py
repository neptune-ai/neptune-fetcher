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
import pathlib
from collections.abc import Collection
from typing import (
    Any,
    Generator,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd

from neptune_fetcher.exceptions import ConflictingAttributeTypes
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.retrieval import (
    metrics,
    series,
)
from neptune_fetcher.internal.retrieval.attribute_types import (
    TYPE_AGGREGATIONS,
    File,
)
from neptune_fetcher.internal.retrieval.attribute_values import AttributeValue
from neptune_fetcher.internal.retrieval.metrics import (
    IsPreviewIndex,
    PreviewCompletionIndex,
    StepIndex,
    TimestampIndex,
    ValueIndex,
)

__all__ = (
    "convert_table_to_dataframe",
    "create_metrics_dataframe",
    "create_series_dataframe",
    "create_files_dataframe",
)


def convert_table_to_dataframe(
    table_data: dict[str, list[AttributeValue]],
    selected_aggregations: dict[identifiers.AttributeDefinition, set[str]],
    type_suffix_in_column_names: bool,
    index_column_name: str = "experiment",
    flatten_file_properties: bool = False,
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
            if value.attribute_definition.type in TYPE_AGGREGATIONS:
                aggregation_value = value.value
                selected_subset = selected_aggregations.get(value.attribute_definition, set())
                aggregations_set = TYPE_AGGREGATIONS[value.attribute_definition.type]

                agg_subset_values = get_aggregation_subset(aggregation_value, selected_subset, aggregations_set)

                for agg_name, agg_value in agg_subset_values.items():
                    row[(column_name, agg_name)] = agg_value
            elif flatten_file_properties and value.attribute_definition.type == "file":
                file_properties: File = value.value
                row[(column_name, "path")] = file_properties.path
                row[(column_name, "size_bytes")] = file_properties.size_bytes
                row[(column_name, "mime_type")] = file_properties.mime_type
            else:
                row[(column_name, "")] = value.value
        return row

    def get_column_name(attr: AttributeValue) -> str:
        return f"{attr.attribute_definition.name}:{attr.attribute_definition.type}"

    def get_aggregation_subset(
        aggregations_value: Any, selected_subset: set[str], aggregations_set: Collection[str]
    ) -> dict[str, Any]:
        result = {}
        for agg_name in aggregations_set:
            if agg_name in selected_subset:
                result[agg_name] = getattr(aggregations_value, agg_name)
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


def create_metrics_dataframe(
    metrics_data: dict[identifiers.RunAttributeDefinition, list[metrics.FloatPointValue]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
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
    sys_id_mapping: dict[str, int] = {}
    label_mapping: list[str] = []

    for run_attr_definition in metrics_data:
        if run_attr_definition.run_identifier.sys_id not in sys_id_mapping:
            sys_id_mapping[run_attr_definition.run_identifier.sys_id] = len(sys_id_mapping)
            label_mapping.append(sys_id_label_mapping[run_attr_definition.run_identifier.sys_id])

        if run_attr_definition.attribute_definition.name not in path_mapping:
            path_mapping[run_attr_definition.attribute_definition.name] = len(path_mapping)

    def generate_categorized_rows() -> Generator[Tuple, None, None]:
        for attribute, points in metrics_data.items():
            exp_category = sys_id_mapping[attribute.run_identifier.sys_id]
            path_category = path_mapping[attribute.attribute_definition.name]

            for point in points:
                # Only include columns that we know we need. Note that the list of columns must match the
                # the list of `types` below.
                head = (
                    exp_category,
                    path_category,
                    point[StepIndex],
                    point[ValueIndex],
                )
                if include_point_previews and timestamp_column_name:
                    tail: Tuple[Any, ...] = (
                        point[TimestampIndex],
                        point[IsPreviewIndex],
                        point[PreviewCompletionIndex],
                    )
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
        np.fromiter(generate_categorized_rows(), dtype=types),
    )

    experiment_dtype = pd.CategoricalDtype(categories=label_mapping)
    df[index_column_name] = pd.Categorical.from_codes(df[index_column_name], dtype=experiment_dtype)
    if timestamp_column_name:
        df[timestamp_column_name] = pd.to_datetime(df[timestamp_column_name], unit="ms", origin="unix", utc=True)

    df = _pivot_and_reindex_df(df, include_point_previews, index_column_name, timestamp_column_name)
    df = _restore_path_column_names(df, path_mapping, "float_series" if type_suffix_in_column_names else None)
    df = _sort_indices(df)

    return df


def create_series_dataframe(
    series_data: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
    index_column_name: str,
    timestamp_column_name: Optional[str],
) -> pd.DataFrame:
    experiment_mapping: dict[identifiers.SysId, int] = {}
    path_mapping: dict[str, int] = {}
    label_mapping: list[str] = []

    for run_attr_definition in series_data.keys():
        if run_attr_definition.run_identifier.sys_id not in experiment_mapping:
            experiment_mapping[run_attr_definition.run_identifier.sys_id] = len(experiment_mapping)
            label_mapping.append(sys_id_label_mapping[run_attr_definition.run_identifier.sys_id])

        if run_attr_definition.attribute_definition.name not in path_mapping:
            path_mapping[run_attr_definition.attribute_definition.name] = len(path_mapping)

    def generate_categorized_rows() -> Generator[Tuple, None, None]:
        for attribute, values in series_data.items():
            exp_category = experiment_mapping[attribute.run_identifier.sys_id]
            path_category = path_mapping[attribute.attribute_definition.name]

            if timestamp_column_name:
                for point in values:
                    yield exp_category, path_category, point.step, point.value, point.timestamp_millis
            else:
                for point in values:
                    yield exp_category, path_category, point.step, point.value

    types = [
        (index_column_name, "uint32"),
        ("path", "uint32"),
        ("step", "float64"),
        ("value", "object"),
    ]
    if timestamp_column_name:
        types.append((timestamp_column_name, "uint64"))

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(), dtype=types),
    )

    experiment_dtype = pd.CategoricalDtype(categories=label_mapping)
    df[index_column_name] = pd.Categorical.from_codes(df[index_column_name], dtype=experiment_dtype)
    if timestamp_column_name:
        df[timestamp_column_name] = pd.to_datetime(df[timestamp_column_name], unit="ms", origin="unix", utc=True)

    df = _pivot_and_reindex_df(df, False, index_column_name, timestamp_column_name)
    df = _restore_path_column_names(df, path_mapping, None)
    df = _sort_indices(df)

    return df


def _pivot_and_reindex_df(
    df: pd.DataFrame,
    include_point_previews: bool,
    index_column_name: str,
    timestamp_column_name: Optional[str],
) -> pd.DataFrame:
    if df.empty and timestamp_column_name:
        # Handle empty DataFrame case to avoid pandas dtype errors
        df[timestamp_column_name] = pd.Series(dtype="datetime64[ns]")

    if include_point_previews or timestamp_column_name:
        # if there are multiple value columns, don't specify them and rely on pandas to create the column multi-index
        df = df.pivot(index=[index_column_name, "step"], columns="path")
    else:
        # when there's only "value", define values explicitly, to make pandas generate a flat index
        df = df.pivot(index=[index_column_name, "step"], columns="path", values="value")

    df = df.reset_index()
    df[index_column_name] = df[index_column_name].astype(str)
    df = df.sort_values(by=[index_column_name, "step"], ignore_index=True)
    df = df.set_index([index_column_name, "step"])

    return df


def _restore_path_column_names(
    df: pd.DataFrame, path_mapping: dict[str, int], type_suffix: Optional[str]
) -> pd.DataFrame:
    """
    Accepts an DF in an intermediate format in _create_dataframe, and the mapping of column names.
    Restores colum names in the DF based on the mapping.
    """

    # We need to reverse the mapping to index -> column name
    if type_suffix:
        reverse_mapping = {index: f"{path}:{type_suffix}" for path, index in path_mapping.items()}
    else:
        reverse_mapping = {index: path for path, index in path_mapping.items()}
    return df.rename(columns=reverse_mapping)


def _sort_indices(df: pd.DataFrame) -> pd.DataFrame:
    # MultiIndex DFs need to have column index order swapped: value/metric_name -> metric_name/value.
    # We also sort columns, but only after the original names have been restored.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns.names = (None, None)
        df = df.swaplevel(axis=1)
        return df.sort_index(axis=1, level=0)
    else:
        df.columns.name = None
        return df.sort_index(axis=1)


def create_files_dataframe(
    files_data: list[tuple[identifiers.RunIdentifier, identifiers.AttributeDefinition, Optional[pathlib.Path]]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
    index_column_name: str = "experiment",
) -> pd.DataFrame:
    if not files_data:
        return pd.DataFrame(
            index=pd.Index([], name=index_column_name),
        )

    rows: list[dict[str, Optional[str]]] = []
    for run_identifier, attribute_definition, target_path in files_data:
        row = {
            index_column_name: sys_id_label_mapping[run_identifier.sys_id],
            "attribute": attribute_definition.name,
            "file_path": str(target_path) if target_path else None,
        }
        rows.append(row)

    dataframe = pd.DataFrame(rows)
    dataframe = dataframe.pivot(index=[index_column_name], columns="attribute", values="file_path")

    sorted_columns = sorted(dataframe.columns)
    return dataframe[sorted_columns]
