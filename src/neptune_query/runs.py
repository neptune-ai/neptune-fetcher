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
    resolve_runs_filter,
    resolve_sort_by,
)
from neptune_query.internal.composition import fetch_metrics as _fetch_metrics
from neptune_query.internal.composition import fetch_series as _fetch_series
from neptune_query.internal.composition import fetch_table as _fetch_table
from neptune_query.internal.composition import list_attributes as _list_attributes
from neptune_query.internal.composition import list_containers as _list_containers
from neptune_query.internal.retrieval import search as _search


def list_runs(
    *,
    project: Optional[str] = None,
    runs: Optional[Union[str, list[str], filters.Filter]] = None,
) -> list[str]:
    """Lists the IDs of runs in a Neptune project.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        runs: Filter specifying which runs to include.
            If a string is provided, it's treated as a regex pattern that the run IDs must match.
            If a list of strings is provided, it's treated as exact run IDs to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
    """
    project_identifier = get_default_project_identifier(project)
    runs_filter = resolve_runs_filter(runs)

    return _list_containers.list_containers(
        project_identifier=project_identifier,
        filter_=runs_filter,
        container_type=_search.ContainerType.RUN,
    )


def list_attributes(
    *,
    project: Optional[str] = None,
    runs: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]] = None,
) -> list[str]:
    """Lists attributes in the runs of a Neptune project.

    To narrow the results, define filters for runs to search or attributes to include.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        runs: Filter specifying which runs to include.
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
        import neptune_query.runs as nq


        nq.list_attributes(attributes=r"^metrics")
        ```

        Search a specific project for runs with a learning rate less than 0.01 and
        return all attributes nested under the "config" namespace:
        ```
        from neptune_query import Filter


        nq.list_attributes(
            project="team-alpha/sandbox",
            runs=Filter.lt("config/lr", 0.01),
            attributes=r"^config/",
        )
        ```
    """

    project_identifier = get_default_project_identifier(project)
    runs_filter = resolve_runs_filter(runs)
    attributes_filter = resolve_attributes_filter(attributes)

    return _list_attributes.list_attributes(
        project_identifier=project_identifier,
        filter_=runs_filter,
        attributes=attributes_filter,
        container_type=_search.ContainerType.RUN,
    )


def fetch_metrics(
    *,
    project: Optional[str] = None,
    runs: Union[str, list[str], filters.Filter],
    attributes: Union[str, list[str], filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    include_point_previews: bool = False,
) -> _pandas.DataFrame:
    """Metric values per step.

    The values are raw, without any aggregation, approximation, or interpolation.

    To narrow the results, limit the step range or the number of values from the tail end.
    You can also define filters for runs to search or attributes to include.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        runs: Filter specifying which runs to include.
            If a string is provided, it's treated as a regex pattern that the run IDs must match.
            If a list of strings is provided, it's treated as exact run IDs to match.
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
        Fetch losses of a specific run from step 1000 onward, including incomplete points:
        ```
        import neptune_query.runs as nq


        nq.fetch_metrics(
            runs=["prompt-wolf-20250605132116671-2g2r1"],
            attributes=r"^loss/.*",
            step_range=(1000.0, None),
            include_point_previews=True,
        )
        ```
    """
    project_identifier = get_default_project_identifier(project)
    runs_filter = resolve_runs_filter(runs)
    attributes_filter = resolve_attributes_filter(attributes)

    return _fetch_metrics.fetch_metrics(
        project_identifier=project_identifier,
        filter_=runs_filter,
        attributes=attributes_filter,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        container_type=_search.ContainerType.RUN,
    )


def fetch_runs_table(
    *,
    project: Optional[str] = None,
    runs: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Union[str, list[str], filters.AttributeFilter] = [],
    sort_by: Union[str, filters.Attribute] = filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
) -> _pandas.DataFrame:
    """Run metadata, with runs as rows and attributes as columns.

    To narrow the results, define filters for runs to search or attributes to include.

    Returns a DataFrame similar to the runs table in the web app.
    For series attributes, the last logged value is returned.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        runs: Filter specifying which runs to include.
            If a string is provided, it's treated as a regex pattern that the run IDs must match.
            If a list of strings is provided, it's treated as exact run IDs to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        sort_by: Name of the attribute to sort the table by.
            Alternatively, an Attribute object that specifies the attribute type.
        sort_direction: The direction to sort columns by: `"desc"` (default) or `"asc"`.
        limit: Maximum number of runs to return. By default, all runs are included.
        type_suffix_in_column_names: If True, columns of the returned DataFrame
            are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
            If False (default), the method throws an exception if there are multiple types under one path.

    Example:
        Fetch attributes matching `loss` or `configs` from a specific run:
        ```
        import neptune_query.runs as nq


        nq.fetch_runs_table(
            runs=["prompt-wolf-20250605132116671-2g2r1"],
            attributes=r"loss|configs",
        )
        ```
    """
    project_identifier = get_default_project_identifier(project)
    runs_filter = resolve_runs_filter(runs)
    attributes_filter = resolve_attributes_filter(attributes)
    resolved_sort_by = resolve_sort_by(sort_by)

    return _fetch_table.fetch_table(
        project_identifier=project_identifier,
        filter_=runs_filter,
        attributes=attributes_filter,
        sort_by=resolved_sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=_search.ContainerType.RUN,
        flatten_aggregations=True,
    )


def fetch_series(
    *,
    project: Optional[str] = None,
    runs: Union[str, list[str], filters.Filter],
    attributes: Union[str, list[str], filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
) -> _pandas.DataFrame:
    """Series values per step, for non-numerical series attributes.

    To narrow the results, define filters for runs to search or attributes to include.

    Supports series of histograms, files, and strings.

    Args:
        project: Path of the Neptune project, as `WorkspaceName/ProjectName`.
            If not provided, the NEPTUNE_PROJECT environment variable is used.
        runs: Filter specifying which runs to include.
            If a string is provided, it's treated as a regex pattern that the run IDs must match.
            If a list of strings is provided, it's treated as exact run IDs to match.
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
        Fetch custom string series of a specific run from step 1000 onward:
        ```
        import neptune_query.runs as nq


        nq.fetch_series(
            runs=["prompt-wolf-20250605132116671-2g2r1"],
            attributes=r"^messages/",
            step_range=(1000.0, None),
        )
        ```
    """
    project_identifier = get_default_project_identifier(project)
    runs_filter = resolve_runs_filter(runs)
    attributes_filter = resolve_attributes_filter(attributes)

    return _fetch_series.fetch_series(
        project_identifier=project_identifier,
        filter_=runs_filter,
        attributes=attributes_filter,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        container_type=_search.ContainerType.RUN,
    )
