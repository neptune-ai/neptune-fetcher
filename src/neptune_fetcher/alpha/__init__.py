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
    "Context",
    "get_context",
    "set_api_token",
    "set_context",
    "set_project",
    "list_experiments",
    "list_attributes",
    "fetch_experiments_table",
    "fetch_metrics",
    "fetch_series",
    "download_files",
]

from typing import (
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_fetcher.alpha import filters as _filters
from neptune_fetcher.alpha.internal import util as _util
from neptune_fetcher.alpha.internal.composition import download_files as _download_files
from neptune_fetcher.alpha.internal.composition import fetch_metrics as _fetch_metrics
from neptune_fetcher.alpha.internal.composition import fetch_series as _fetch_series
from neptune_fetcher.alpha.internal.composition import fetch_table as _fetch_table
from neptune_fetcher.alpha.internal.composition import list_attributes as _list_attributes
from neptune_fetcher.alpha.internal.composition import list_containers as _list_containers
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    set_api_token,
    set_context,
    set_project,
)
from neptune_fetcher.alpha.internal.retrieval import search as _search


def list_experiments(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
     Returns a list of experiment names in a project.

    `experiments` - a filter specifying which experiments to include
         - a regex that experiment name must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """
    experiments = _util.resolve_experiments_filter(experiments)

    return _list_containers.list_containers(experiments, context, _search.ContainerType.EXPERIMENT)


def list_attributes(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], _filters.AttributeFilter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
    List attributes' names in project.
    Optionally filter by experiments and attributes.
    `experiments` - a filter specifying experiments to which the attributes belong
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
            If `AttributeFilter.aggregations` is set, an exception will be raised as they're
            not supported in this function.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a list of unique attribute names in experiments matching the filter.
    """

    experiments = _util.resolve_experiments_filter(experiments)
    attributes = _util.resolve_attributes_filter(attributes)

    return _list_attributes.list_attributes(
        experiments, attributes, context, container_type=_search.ContainerType.EXPERIMENT
    )


def fetch_metrics(
    experiments: Union[str, list[str], _filters.Filter],
    attributes: Union[str, list[str], _filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    include_point_previews: bool = False,
    context: Optional[Context] = None,
) -> _pandas.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation).

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
    `include_point_previews` - False by default. If False the returned results will only contain committed
        points. If True the results will also include preview points and the returned DataFrame will
        have additional sub-columns with preview status (is_preview and preview_completion).

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    experiments_ = _util.resolve_experiments_filter(experiments)
    assert experiments_ is not None
    attributes = _util.resolve_attributes_filter(attributes, forced_type=["float_series"])

    return _fetch_metrics.fetch_metrics(
        filter_=experiments_,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        context=context,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def fetch_experiments_table(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Union[str, list[str], _filters.AttributeFilter] = "^sys/name$",
    sort_by: Union[str, _filters.Attribute] = _filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> _pandas.DataFrame:
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
    experiments = _util.resolve_experiments_filter(experiments)
    attributes = _util.resolve_attributes_filter(attributes)
    sort_by = _util.resolve_sort_by(sort_by)

    return _fetch_table.fetch_table(
        filter_=experiments,
        attributes=attributes,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def fetch_series(
    experiments: Union[str, list[str], _filters.Filter],
    attributes: Union[str, list[str], _filters.AttributeFilter],
    *,
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    context: Optional[Context] = None,
) -> _pandas.DataFrame:
    """
    Fetches raw values for string series from selected experiments.

    Currently only supports attributes of type string_series.

    `experiments` - a filter specifying which experiments to include
        - a regex that experiment name must match, or
        - a Filter object for more complex filtering
    `attributes` - a filter specifying which attributes to include
        - a regex that attribute name must match, or
        - an AttributeFilter object
    `include_time` - whether to include absolute timestamp
    `step_range` - tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, maximum number of points to include.
    `context` - context object to be used; primarily useful for switching projects

    Returns a DataFrame containing string series for the specified experiments and attributes.
    If include_time is set, each series column will have an additional sub-column with the requested timestamp values.
    """
    experiments_ = _util.resolve_experiments_filter(experiments)
    assert experiments_ is not None
    attributes = _util.resolve_attributes_filter(attributes, forced_type=["string_series"])

    return _fetch_series.fetch_series(
        filter_=experiments_,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        context=context,
        container_type=_search.ContainerType.EXPERIMENT,
    )


def download_files(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], _filters.AttributeFilter]] = None,
    *,
    destination: Optional[str] = None,
    context: Optional[Context] = None,
) -> _pandas.DataFrame:
    """
    Downloads files associated with selected experiments and attributes.

    `experiments` - a filter specifying which runs to include in the table
        - a regex that the run ID must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that the attribute name must match, or
        - an AttributeFilter object
    `destination`: the directory where files will be downloaded.
        - If `None`, the current working directory (CWD) is used as the default.
        - The path can be relative or absolute.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a DataFrame mapping experiments and attributes to the paths of downloaded files.
    """
    experiments = _util.resolve_experiments_filter(experiments)
    attributes = _util.resolve_attributes_filter(attributes, forced_type=["file"])
    destination_path = _util.resolve_destination_path(destination)

    return _download_files.download_files(
        filter_=experiments,
        attributes=attributes,
        destination=destination_path,
        context=context,
        container_type=_search.ContainerType.EXPERIMENT,
    )
