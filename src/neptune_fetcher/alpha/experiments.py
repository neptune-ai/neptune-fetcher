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
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.api_client import get_client
from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs
from neptune_fetcher.alpha.internal.identifiers import ProjectIdentifier
from neptune_fetcher.alpha.internal.metric import fetch_flat_dataframe_metrics


def fetch_experiments_table(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    attributes: Union[str, AttributeFilter] = "^sys/name$",
    sort_by: Union[str, Attribute] = Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    `experiments` - a filter specifying which experiments to include in the table
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object
    `sort_by` - an attribute name or an Attribute object specifying type and, optionally, aggregation
    `sort_direction` - 'asc' or 'desc'
    `limit` - maximum number of experiments to return; by default all experiments are returned.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a DataFrame similar to the Experiments Table in the UI, with an important difference:
    aggregates of metrics (min, max, avg, last, ...) are returned as sub-columns of a metric column. In other words,
    the returned DataFrame is indexed with a MultiIndex on (attribute name, attribute property).

    In case the user doesn't specify metrics' aggregates to be returned, only the `last` aggregate is returned.
    """

    pass


def fetch_metrics(
    experiments: Union[str, ExperimentFilter],
    attributes: Union[str, AttributeFilter],
    include_timestamp: Optional[Literal["relative", "absolute"]] = None,
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
    `include_timestamp` - whether to include relative or absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.

    If `include_timestamp` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    valid_context = validate_context(context or get_context())
    client = get_client(valid_context)

    experiments = (
        ExperimentFilter.matches_all(Attribute("sys/name", type="string"), regex=experiments)
        if isinstance(experiments, str)
        else experiments
    )
    attributes = (
        AttributeFilter(name_matches_all=attributes, type_in=["float_series"])
        if isinstance(attributes, str)
        else attributes
    )

    df = fetch_flat_dataframe_metrics(
        experiments=experiments,
        attributes=attributes,
        client=client,
        project=ProjectIdentifier(valid_context.project),  # type: ignore
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
    )
    if df.empty:
        return df

    if include_timestamp == "absolute":
        df = df.rename(columns={"timestamp": "absolute_time"})
        df = df.pivot(
            index=["experiment", "step"],
            columns="path",
            values=["value", "absolute_time"],
        )

        df = df.swaplevel(axis=1)
        if type_suffix_in_column_names:
            df = df.rename(columns=lambda x: x + ":float_series", level=0, copy=False)

        df = df.sort_index(axis=1, level=0)
        df = df.reset_index()
        return df
    elif include_timestamp == "relative":
        raise NotImplementedError("Relative timestamp is not implemented")
    else:
        df = df.pivot(index=["experiment", "step"], columns="path", values="value")
        if type_suffix_in_column_names:
            df = df.rename(columns=lambda x: x + ":float_series", copy=False)

        df = df.reset_index()
        df = df.sort_index(axis=1)
        df = df.rename_axis(None, axis=1)
        return df


def list_experiments(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
     Returns a list of experiment names in a project.

    `experiments` - a filter specifying which experiments to include
         - a regex that experiment name must match, or
         - a Filter object
    """

    validated_context = validate_context(context or get_context())
    client = get_client(validated_context)

    if isinstance(experiments, str):
        experiments = ExperimentFilter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    assert validated_context.project is not None  # mypy
    pages = fetch_experiment_sys_attrs(client, ProjectIdentifier(validated_context.project), experiments)
    return list(exp.sys_name for page in pages for exp in page.items)
