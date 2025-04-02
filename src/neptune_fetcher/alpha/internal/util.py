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
    Optional,
    Union,
)

from neptune_fetcher.alpha import filters as _filters


def resolve_runs_filter(runs: Optional[Union[str, list[str], _filters.Filter]]) -> Optional[_filters.Filter]:
    if isinstance(runs, str):
        return _filters.Filter.matches_all(_filters.Attribute("sys/custom_run_id", type="string"), regex=runs)
    if isinstance(runs, list):
        return _filters.Filter.any(
            *[_filters.Filter.eq(_filters.Attribute("sys/custom_run_id", type="string"), value=run) for run in runs]
        )
    return runs


def resolve_experiments_filter(
    experiments: Optional[Union[str, list[str], _filters.Filter]]
) -> Optional[_filters.Filter]:
    if isinstance(experiments, str):
        return _filters.Filter.matches_all(_filters.Attribute("sys/name", type="string"), experiments)
    if isinstance(experiments, list):
        return _filters.Filter.name_in(*experiments)
    return experiments


def resolve_attributes_filter(
    attributes: Optional[Union[str, list[str], _filters.AttributeFilter]]
) -> _filters.AttributeFilter:
    if attributes is None:
        return _filters.AttributeFilter()
    if isinstance(attributes, str):
        return _filters.AttributeFilter(name_matches_all=attributes)
    if isinstance(attributes, list):
        return _filters.AttributeFilter(name_eq=attributes)
    return attributes


def resolve_sort_by(sort_by: Union[str, _filters.Attribute]) -> _filters.Attribute:
    if isinstance(sort_by, str):
        return _filters.Attribute(sort_by)
    return sort_by
