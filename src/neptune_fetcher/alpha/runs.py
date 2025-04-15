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
]

from typing import (
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_fetcher import filters as _filters
from neptune_fetcher import runs as _runs
from neptune_fetcher.internal import context as _context


def list_runs(
    runs: Optional[Union[str, list[str], _filters.Filter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
    Deprecated. Use `list_runs` from the `neptune_fetcher.runs` package instead.
    """
    return _runs.list_runs(runs=runs, context=context)


def list_attributes(
    runs: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], _filters.AttributeFilter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
    Deprecated. Use `list_attributes` from the `neptune_fetcher.runs` package instead.
    """
    return _runs.list_attributes(
        runs=runs,
        attributes=attributes,
        context=context,
    )


def fetch_metrics(
    runs: Union[str, list[str], _filters.Filter],
    attributes: Union[str, list[str], _filters.AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    include_point_previews: bool = False,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    """
    Deprecated. Use `fetch_metrics` from the `neptune_fetcher.runs` package instead.
    """
    return _runs.fetch_metrics(
        runs=runs,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        context=context,
    )


def fetch_runs_table(
    runs: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Union[str, list[str], _filters.AttributeFilter] = "^sys/name$",
    sort_by: Union[str, _filters.Attribute] = _filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[_context.Context] = None,
) -> _pandas.DataFrame:
    """
    Deprecated. Use `fetch_runs_table` from the `neptune_fetcher.runs` package instead.
    """
    return _runs.fetch_runs_table(
        runs=runs,
        attributes=attributes,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
    )
