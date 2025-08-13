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
    "download_files",
]

import pathlib
from typing import (
    Iterable,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_query import (
    filters,
    types,
)
from neptune_query._internal import (
    get_default_project_identifier,
    resolve_attributes_filter,
    resolve_destination_path,
    resolve_experiments_filter,
    resolve_files,
    resolve_sort_by,
)
from neptune_query.exceptions import NeptuneUserError
from neptune_query.internal.composition import download_files as _download_files
from neptune_query.internal.composition import fetch_metrics as _fetch_metrics
from neptune_query.internal.composition import fetch_series as _fetch_series
from neptune_query.internal.composition import fetch_table as _fetch_table
from neptune_query.internal.composition import list_attributes as _list_attributes
from neptune_query.internal.composition import list_containers as _list_containers
from neptune_query.internal.context import set_api_token
from neptune_query.internal.query_metadata_context import use_query_metadata
from neptune_query.internal.retrieval import search as _search


@use_query_metadata(api_function="list_experiments")
def list_experiments(
    *,
    project: Optional[str] = None,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
) -> list[str]:
    """Lists the names of experiments in a Neptune project.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.

    Examples:
        List all experiments whose names begin with "sigurd":
        ```
        import neptune_query as nq


        nq.list_experiments(experiments=r"^sigurd")
        ```

        Search a specific project for experiments with a learning rate less than 0.01:
        ```
        from neptune_query.filters import Filter


        nq.list_experiments(
            project="team-alpha/sandbox",
            experiments=Filter.lt("config/lr", 0.01),
        )
        ```
    """
    project_identifier = get_default_project_identifier(project)
    experiments_filter = resolve_experiments_filter(experiments)

    return _list_containers.list_containers(
        project_identifier=project_identifier,
        filter_=experiments_filter,
        container_type=_search.ContainerType.EXPERIMENT,
    )


@use_query_metadata(api_function="list_attributes")
def list_attributes(
    *,
    project: Optional[str] = None,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]] = None,
) -> list[str]:
    """Lists attributes in the experiments of a Neptune project.

    To narrow the results, define filters for experiments to search or attributes to include.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.

    Examples:
        List all attributes that begin with "metrics":
        ```
        import neptune_query as nq


        nq.list_attributes(attributes=r"^metrics")
        ```

        Search a specific project for experiments with a learning rate less than 0.01 and
        return the logged attributes:
        ```
        from neptune_query.filters import Filter


        nq.list_attributes(
            project="team-alpha/sandbox",
            experiments=Filter.lt("config/lr", 0.01),
        )
        ```
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


@use_query_metadata(api_function="fetch_metrics")
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
    """Fetches a table of metric values per step.

    The values are raw, without any aggregation, approximation, or interpolation.

    To narrow the results, limit the step range or the number of values from the tail end.
    You can also define filters for experiments to search or attributes to include.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        include_time: To include absolute timestamps, pass `"absolute"` as the value.
            If set, each metric column has an additional sub-column with requested timestamp values.
        step_range: Tuple specifying the range of steps to include. Can represent an open interval.
        lineage_to_the_root: If True (default), includes all points from the complete experiment history.
            If False, only includes points from the most recent experiment in the lineage.
        tail_limit: From the tail end of each series, how many points to include at most.
        type_suffix_in_column_names: If True, columns of the returned DataFrame
            are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
            If False (default), the method throws an exception if there are multiple types under one path.
        include_point_previews: If False (default), the returned results only contain committed
            points. If True, the results also include preview points and the returned DataFrame will
            have additional sub-columns with preview status (is_preview and preview_completion).

    Example:
        Fetch losses of two specific experiments from step 1000 onward, including incomplete points:
        ```
        import neptune_query as nq


        nq.fetch_metrics(
            experiments=["seagull-week1", "seagull-week2"],
            attributes=r"^loss/.*",
            step_range=(1000.0, None),
            include_point_previews=True,
        )
        ```
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


@use_query_metadata(api_function="fetch_experiments_table")
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
    """Fetches a table of experiment metadata, with runs as rows and attributes as columns.

    To narrow the results, define filters for experiments to search or attributes to include.

    Returns a DataFrame similar to the runs table in the web app.
    For series attributes, the last logged value is returned.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        sort_by: Name of the attribute to sort the table by.
            Alternatively, an Attribute object that specifies the attribute type.
        sort_direction: The direction to sort columns by: `"desc"` (default) or `"asc"`.
        limit: Maximum number of experiments to return. By default, all experiments are included.
        type_suffix_in_column_names: If True, columns of the returned DataFrame
            are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
            If False (default), the method throws an exception if there are multiple types under one path.

    Example:
        Fetch attributes matching `loss` or `configs` from two specific experiments:
        ```
        import neptune_query as nq


        nq.fetch_experiments_table(
            experiments=["seagull-week1", "seagull-week2"],
            attributes=r"loss | configs",
        )
        ```
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


@use_query_metadata(api_function="fetch_series")
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
    """Fetches a table of series values per step, for non-numerical series attributes.

    To narrow the results, define filters for experiments to search or attributes to include.

    Supports series of histograms, files, and strings.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        include_time: To include absolute timestamps, pass `"absolute"` as the value.
            If set, each metric column has an additional sub-column with requested timestamp values.
        step_range: Tuple specifying the range of steps to include. Can represent an open interval.
        lineage_to_the_root: If True (default), includes all values from the complete experiment history.
            If False, only includes values from the most recent experiment in the lineage.
        tail_limit: From the tail end of each series, how many values to include at most.

    Example:
        Fetch custom string series of two specific experiments from step 1000 onward:
        ```
        import neptune_query as nq


        nq.fetch_series(
            experiments=["seagull-week1", "seagull-week2"],
            attributes=r"^messages/",
            step_range=(1000.0, None),
        )
        ```
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


@use_query_metadata(api_function="download_files")
def download_files(
    *,
    files: Union[types.File, Iterable[types.File], _pandas.Series, _pandas.DataFrame],
    destination: Optional[Union[str, pathlib.Path]] = None,
) -> _pandas.DataFrame:
    """Downloads files from a Neptune project.

    - For file series, use `fetch_series()` to specify the content containing the files
    and pass the output to the `files` argument.
    - For individually assigned files, use `fetch_experiments_table()`.

    Args:
        files: Which files to download, specified using one of the following options.
            - File object
            - Iterable of File objects
            - pandas Series containing, non-exclusively, File objects
            - pandas DataFrame containing, among other data, File objects
        destination: Directory where files will be downloaded.
            The path can be relative or absolute.
            If `None`, the current working directory (CWD) is used as the default.

    Returns:
        DataFrame mapping experiments and attributes to the paths of downloaded files.

    Example:
        Specify files from a given step range of a series:
        ```
        import neptune_query as nq


        interesting_files = nq.fetch_series(
            experiments=["seagull-week1", "seagull-week2"],
            attributes=r"^predictions/",
            step_range=(2050.0, 2056.0),
        )

        nq.download_files(files=interesting_files)
        ```
    """
    file_list = resolve_files(files)
    destination_path = resolve_destination_path(destination)

    if not all(file.experiment_name for file in file_list):
        raise NeptuneUserError(
            "Some files passed to nq.download_files don't have associated experiment names. "
            "This is likely because you passed files from the runs API. "
            "Please use files from the experiments API instead by fetching them with "
            "nq.fetch_series() or nq.fetch_experiments_table()."
        )

    return _download_files.download_files(
        files=file_list,
        destination=destination_path,
        container_type=_search.ContainerType.EXPERIMENT,
    )
