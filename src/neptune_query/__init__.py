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
#

__all__ = [
    "set_api_token",
    "list_experiments",
    "list_attributes",
    "fetch_experiments_table",
    "fetch_metrics",
    "fetch_series",
]

from typing import (
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_query import filters
from neptune_query._internal import (
    get_default_project_identifier,
    resolve_attributes_filter,
    resolve_experiments_filter,
    resolve_sort_by,
)
from neptune_query.internal.composition import fetch_metrics as _fetch_metrics
from neptune_query.internal.composition import fetch_series as _fetch_series
from neptune_query.internal.composition import fetch_table as _fetch_table
from neptune_query.internal.composition import list_attributes as _list_attributes
from neptune_query.internal.composition import list_containers as _list_containers
from neptune_query.internal.context import set_api_token
from neptune_query.internal.retrieval import search as _search


def list_experiments(
    *,
    project: Optional[str] = None,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
) -> list[str]:
    """
     Returns a list of experiment names in a project.

    `project` - the project name to use; if not provided, NEPTUNE_PROJECT env var is used
    `experiments` - a filter specifying which experiments to include
        - a list of specific experiment names, or
        - a regex that the experiment name must match, or
        - a Filter object
    """
    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)

    return _list_containers.list_containers(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def list_attributes(
    *,
    project: Optional[str] = None,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]] = None,
) -> list[str]:
    """
    List attributes' names in project.
    Optionally filter by experiments and attributes.

    `project` - the project name to use; if not provided, NEPTUNE_PROJECT env var is used
    `experiments` - a filter specifying experiments to which the attributes belong
        - a list of specific experiment names, or
        - a regex that the experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a list of specific attribute names, or
        - a regex that attribute name must match, or
        - an AttributeFilter object;

    Returns a list of unique attribute names in experiments matching the filter.
    """

    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)
    attributes_filter = resolve_attributes_filter(attributes)

    return _list_attributes.list_attributes(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        attributes=attributes_filter,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def fetch_metrics(
    *,
    project: Optional[str] = None,
    experiments: Union[str, list[str], filters.Filter],
    attributes: Union[str, list[str], filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    include_point_previews: bool = False,
) -> _pandas.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation).

    `project` - the project name to use; if not provided, NEPTUNE_PROJECT env var is used
    `experiments` - a filter specifying which experiments to include
        - a list of specific experiment names, or
        - a regex that the experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a list of specific attribute names, or
        - a regex that attribute name must match, or
        - an AttributeFilter object;
    `include_time` - whether to include absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.
    `include_point_previews` - False by default. If False the returned results will only contain committed
        points. If True the results will also include preview points and the returned DataFrame will
        have additional sub-columns with preview status (is_preview and preview_completion).

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)
    attributes_filter = resolve_attributes_filter(attributes)

    return _fetch_metrics.fetch_metrics(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        attributes=attributes_filter,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def fetch_experiments_table(
    *,
    project: Optional[str] = None,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Union[str, list[str], filters.AttributeFilter] = [],
    sort_by: Union[str, filters.Attribute] = filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
) -> _pandas.DataFrame:
    """
    `project` - the project name to use; if not provided, NEPTUNE_PROJECT env var is used
    `experiments` - a filter specifying which experiments to include in the table
        - a list of specific experiment names, or
        - a regex that the experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a list of specific attribute names, or
        - a regex that attribute name must match, or
        - an AttributeFilter object;
    `sort_by` - an attribute name or an Attribute object specifying type
    `sort_direction` - 'asc' or 'desc'
    `limit` - maximum number of experiments to return; by default all experiments are returned.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.

    Returns a DataFrame similar to the Experiments Table in the UI.
    (Only the last logged value of each metric is returned, no aggregations or approximations)
    """
    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)
    attributes_filter = resolve_attributes_filter(attributes)
    resolved_sort_by = resolve_sort_by(sort_by)

    return _fetch_table.fetch_table(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        attributes=attributes_filter,
        sort_by=resolved_sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=_search.ContainerType.EXPERIMENT,
        flatten_aggregations=True,
    )


def fetch_series(
    *,
    project: Optional[str] = None,
    experiments: Union[str, list[str], filters.Filter],
    attributes: Union[str, list[str], filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
) -> _pandas.DataFrame:
    """
    Fetches raw values for string series from selected experiments.

    Currently only supports attributes of type string_series.

    `project` - the project name to use; if not provided, NEPTUNE_PROJECT env var is used
    `experiments` - a filter specifying which experiments to include
        - a list of specific experiment names, or
        - a regex that the experiment name must match, or
        - a Filter object for more complex filtering
    `attributes` - a filter specifying which attributes to include in the table
        - a list of specific attribute names, or
        - a regex that attribute name must match, or
        - an AttributeFilter object;
    `include_time` - whether to include absolute timestamp
    `step_range` - tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, maximum number of points to include.
    `context` - context object to be used; primarily useful for switching projects

    Returns a DataFrame containing string series for the specified experiments and attributes.
    If include_time is set, each series column will have an additional sub-column with the requested timestamp values.
    """
    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)
    attributes_filter = resolve_attributes_filter(attributes)

    return _fetch_series.fetch_series(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        attributes=attributes_filter,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        container_type=_search.ContainerType.EXPERIMENT,
    )
