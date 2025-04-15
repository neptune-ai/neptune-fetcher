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
]

from typing import (
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as _pandas

from neptune_fetcher import Context
from neptune_fetcher import fetch_experiments_table as _fetch_experiments_table
from neptune_fetcher import fetch_metrics as _fetch_metrics
from neptune_fetcher import filters as _filters
from neptune_fetcher import get_context
from neptune_fetcher import list_attributes as _list_attributes
from neptune_fetcher import list_experiments as _list_experiments
from neptune_fetcher import (
    set_api_token,
    set_context,
    set_project,
)
from neptune_fetcher.internal import warnings as _warnings

_warnings.warn_deprecated(
    "The `neptune_fetcher.alpha` package is deprecated and will be removed in a future version. "
    "Import directly from the `neptune_fetcher` package instead of `alpha`.",
)


def list_experiments(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
    The alpha package is deprecated. Use `list_experiments` directly from the `neptune_fetcher` package instead.
    """
    return _list_experiments(
        experiments=experiments,
        context=context,
    )


def list_attributes(
    experiments: Optional[Union[str, list[str], _filters.Filter]] = None,
    attributes: Optional[Union[str, list[str], _filters.AttributeFilter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
    The alpha package is deprecated. Use `list_attributes` directly from the `neptune_fetcher` package instead.
    """
    return _list_attributes(
        experiments=experiments,
        attributes=attributes,
        context=context,
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
    The alpha package is deprecated. Use `fetch_metrics` directly from the `neptune_fetcher` package instead.
    """
    return _fetch_metrics(
        experiments=experiments,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        context=context,
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
    The alpha package is deprecated. Use `fetch_experiments_table` directly from the `neptune_fetcher` package instead.
    """
    return _fetch_experiments_table(
        experiments=experiments,
        attributes=attributes,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
    )
