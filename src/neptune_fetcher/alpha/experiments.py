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

from neptune_fetcher.alpha.context import Context
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)


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
    experiments: str | ExperimentFilter,
    attributes: str | AttributeFilter,
    include_timestamp: Literal["relative", "absolute"] | None = None,
    step_range: Tuple[float | None, float | None] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: int | None = None,
    type_suffix_in_column_names: bool = False,
    context: Context | None = None,
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
