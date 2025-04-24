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
    "list_runs",
    "list_attributes",
    "fetch_runs_table",
    "fetch_metrics",
    "fetch_series",
]

import pathlib as _pathlib
from typing import (
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_fetcher.alpha import filters as _filters
from neptune_fetcher.alpha.internal import context as _context
from neptune_fetcher.alpha.internal.composition import download_files as _download_files
from neptune_fetcher.alpha.internal.composition import fetch_metrics as _fetch_metrics
from neptune_fetcher.alpha.internal.composition import fetch_series as _fetch_series
from neptune_fetcher.alpha.internal.composition import fetch_table as _fetch_table
from neptune_fetcher.alpha.internal.composition import list_attributes as _list_attributes
from neptune_fetcher.alpha.internal.composition import list_containers as _list_containers
from neptune_fetcher.alpha.internal.retrieval import search as _search


def list_runs(
    runs: Optional[Union[str, _filters.Filter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
     Returns a list of run IDs in a project.

    `runs` - a filter specifying which runs to include
         - a regex that the run ID must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), regex=runs)

    return _list_containers.list_containers(runs, context, _search.ContainerType.RUN)


def list_attributes(
    runs: Optional[Union[str, _filters.Filter]] = None,
    attributes: Optional[Union[str, _filters.AttributeFilter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
    List the names of attributes in a project.
    Optionally filter by runs and attributes.
    `runs` - a filter specifying runs to which the attributes belong
        - a regex that the run ID must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that the attribute name must match, or
        - an AttributeFilter object;
            If `AttributeFilter.aggregations` is set, an exception will be raised as they're
            not supported in this function.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a list of unique attribute names in runs matching the filter.
    """
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), regex=runs)

    if attributes is None:
        attributes = _filters.AttributeFilter()
    elif isinstance(attributes, str):
        attributes = _filters.AttributeFilter(name_matches_all=[attributes])

    return _list_attributes.list_attributes(runs, attributes, context, container_type=_search.ContainerType.RUN)


def fetch_metrics(
    runs: Union[str, _filters.Filter],
    attributes: Union[str, _filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    include_point_previews: bool = False,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation).

    `runs` - a filter specifying which runs to include
        - a regex that the run ID must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that the attribute name must match, or
        - an AttributeFilter object;
                If `AttributeFilter.aggregations` is set, an exception will be raised as
                they're not supported in this function.
    `include_time` - whether to include absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete run history.
        If False, only includes points from the most recent run in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If set to True, columns of the returned DataFrame
        are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
        If False, an exception is raised if there are multiple types under one attribute path.
    `include_point_previews` - False by default. If False the returned results will only contain committed
        points. If True the results will also include preview points and the returned DataFrame will
        have additional sub-columns with preview status (is_preview and preview_completion).

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), regex=runs)

    if isinstance(attributes, str):
        attributes = _filters.AttributeFilter(name_matches_all=attributes, type_in=["float_series"])

    return _fetch_metrics.fetch_metrics(
        filter_=runs,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        context=context,
        container_type=_search.ContainerType.RUN,
    )


def fetch_runs_table(
    runs: Optional[Union[str, _filters.Filter]] = None,
    attributes: Union[str, _filters.AttributeFilter] = "^sys/name$",
    sort_by: Union[str, _filters.Attribute] = _filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    """
    `runs` - a filter specifying which runs to include in the table
        - a regex that the run ID must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that the attribute name must match, or
        - an AttributeFilter object
    `sort_by` - an attribute name or an Attribute object specifying type and, optionally, aggregation
    `sort_direction` - 'asc' or 'desc'
    `limit` - maximum number of runs to return; by default all runs are returned.
    `type_suffix_in_column_names` - False by default. If set to True, columns of the returned DataFrame
        are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
        If False, an exception is raised if there are multiple types under one attribute path.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a DataFrame similar to the runs table in the web app, with an important difference:
    aggregates of metrics (min, max, avg, last, ...) are returned as sub-columns of a metric column. In other words,
    the returned DataFrame is indexed with a MultiIndex on (attribute name, attribute property).
    If you don't specify aggregates to return, only the last logged value of each metric is returned.
    """
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), runs)

    if isinstance(attributes, str):
        attributes = _filters.AttributeFilter(name_matches_all=attributes)

    if isinstance(sort_by, str):
        sort_by = _filters.Attribute(sort_by)

    return _fetch_table.fetch_table(
        filter_=runs,
        attributes=attributes,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
        container_type=_search.ContainerType.RUN,
    )


def fetch_series(
    runs: Union[str, _filters.Filter],
    attributes: Union[str, _filters.AttributeFilter],
    *,
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), regex=runs)

    if isinstance(attributes, str):
        attributes = _filters.AttributeFilter(name_matches_all=attributes, type_in=["string_series"])

    return _fetch_series.fetch_series(
        filter_=runs,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        context=context,
        container_type=_search.ContainerType.RUN,
    )


def download_files(
    runs: Optional[Union[str, _filters.Filter]] = None,
    attributes: Optional[Union[str, _filters.AttributeFilter]] = None,
    *,
    destination: Optional[str] = None,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    """
    Downloads files associated with selected experiments and attributes.

    Args:
      runs: a filter specifying which runs to include in the table
        - a regex that the run ID must match, or
        - a Filter object

      attributes: Specifies the attribute(s) to filter files by within the selected experiments.
          - A string representing the attribute path or an `AttributeFilter` object.
          - If `None`, all attributes are considered.

      destination: The directory where files will be downloaded.
          - If `None`, the current working directory (CWD) is used as the default.
          - The path can be relative or absolute.

      context: Provides additional contextual information for the download (optional).
          - A `Context` object, which may include things like credentials or other metadata.

    Download Path Construction:
      - Files are downloaded to the following directory structure:
          <destination>/<experiment_name>/<attribute_path>/<file_name>
      - If `<experiment_name>` or `<attribute_path>` contains '/', corresponding subdirectories will be created.
      - The `<file_name>` is the final part of the file's path on object storage after splitting it by '/'.

    Example:
      Given an experiment named "some/experiment" and an attribute "some/attribute" with an uploaded file path
      of "/my/path/on/object/storage/file.txt":

          download_files(experiments="some/experiment", attributes="some/attribute", destination="/my/destination")

      The file will be downloaded to:

          /my/destination/some/experiment/some/attribute/file.txt

    Notes:
      - If the experiment or attribute paths include slashes ('/'), they will be treated as subdirectory structures,
        and those directories will be created during the download process.
      - Ensure that the `destination` directory has write permissions for successful file downloads.
      - If the specified destination or any subdirectories do not exist, they will be automatically created.
    """
    if isinstance(runs, str):
        runs = _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), runs)

    if isinstance(attributes, str):
        attributes = _filters.AttributeFilter(name_matches_all=attributes, type_in=["file"])
    elif attributes is None:
        attributes = _filters.AttributeFilter(type_in=["file"])

    if destination is None:
        destination_path = _pathlib.Path.cwd()
    else:
        destination_path = _pathlib.Path(destination).resolve()

    return _download_files.download_files(
        filter_=runs,
        attributes=attributes,
        destination=destination_path,
        context=context,
        container_type=_search.ContainerType.RUN,
    )
