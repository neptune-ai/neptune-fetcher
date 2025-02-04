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
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal import infer as _infer
from neptune_fetcher.alpha.internal import util as _util
from neptune_fetcher.alpha.internal.api_client import get_client
from neptune_fetcher.alpha.internal.metric import fetch_flat_dataframe_metrics


def fetch_metrics(
    experiments: Union[str, Filter],
    attributes: Union[str, AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation),
    or single-value attributes. In case of the latter, their historical values are returned.

    `experiments` - a filter specifying which experiments to include
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
                If `AttributeFilter.aggregations` is set, an exception will be raised as
                they're not supported in this function.
    `include_time` - whether to include absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    _validate_step_range(step_range)
    _validate_tail_limit(tail_limit)
    _validate_include_time(include_time)

    valid_context = validate_context(context or get_context())

    client = get_client(valid_context)
    project_identifier = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    experiments = (
        Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)
        if isinstance(experiments, str)
        else experiments
    )
    attributes = (
        AttributeFilter(name_matches_all=attributes, type_in=["float_series"])
        if isinstance(attributes, str)
        else attributes
    )

    with (
        _util.create_thread_pool_executor() as executor,
        _util.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        _infer.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            experiment_filter=experiments,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        df = fetch_flat_dataframe_metrics(
            experiments=experiments,
            attributes=attributes,
            client=client,
            project=project_identifier,
            step_range=step_range,
            lineage_to_the_root=lineage_to_the_root,
            tail_limit=tail_limit,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

    if include_time == "absolute":
        return _transform_with_absolute_timestamp(df, type_suffix_in_column_names)
    # elif include_time == "relative":
    #     raise NotImplementedError("Relative timestamp is not implemented")
    else:
        return _transform_without_timestamp(df, type_suffix_in_column_names)


def _transform_with_absolute_timestamp(df: pd.DataFrame, type_suffix_in_column_names: bool) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", origin="unix", utc=True)
    df = df.rename(columns={"timestamp": "absolute_time"})
    df = df.pivot(
        index=["experiment", "step"],
        columns="path",
        values=["value", "absolute_time"],
    )

    df = df.swaplevel(axis=1)
    if type_suffix_in_column_names:
        df = df.rename(columns=lambda x: x + ":float_series", level=0, copy=False)

    df = df.reset_index()
    df["experiment"] = df["experiment"].astype(str)
    df = df.sort_values(by=["experiment", "step"], ignore_index=True)
    df.columns.names = (None, None)
    df = df.set_index(["experiment", "step"])
    df = df.sort_index(axis=1, level=0)
    return df


def _transform_without_timestamp(df: pd.DataFrame, type_suffix_in_column_names: bool) -> pd.DataFrame:
    df = df.pivot(index=["experiment", "step"], columns="path", values="value")
    if type_suffix_in_column_names:
        df = df.rename(columns=lambda x: x + ":float_series", copy=False)

    df = df.reset_index()
    df["experiment"] = df["experiment"].astype(str)
    df = df.sort_values(by=["experiment", "step"], ignore_index=True)
    df.columns.name = None
    df = df.set_index(["experiment", "step"])
    df = df.sort_index(axis=1)
    return df


def _validate_step_range(step_range: Tuple[Optional[float], Optional[float]]) -> None:
    """Validate that a step range tuple contains valid values and is properly ordered."""
    if not isinstance(step_range, tuple) or len(step_range) != 2:
        raise ValueError("step_range must be a tuple of two values")

    start, end = step_range

    # Validate types
    if start is not None and not isinstance(start, (int, float)):
        raise ValueError("step_range start must be None or a number")
    if end is not None and not isinstance(end, (int, float)):
        raise ValueError("step_range end must be None or a number")

    # Validate range order if both values are provided
    if start is not None and end is not None and start > end:
        raise ValueError("step_range start must be less than or equal to end")


def _validate_tail_limit(tail_limit: Optional[int]) -> None:
    """Validate that tail_limit is either None or a positive integer."""
    if tail_limit is not None:
        if not isinstance(tail_limit, int):
            raise ValueError("tail_limit must be None or an integer")
        if tail_limit <= 0:
            raise ValueError("tail_limit must be greater than 0")


def _validate_include_time(include_time: Optional[Literal["absolute"]]) -> None:
    if include_time is not None:
        if include_time not in ["absolute"]:
            raise ValueError("include_time must be 'absolute'")
