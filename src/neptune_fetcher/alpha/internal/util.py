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
import pathlib
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
    experiments: Optional[Union[str, list[str], _filters.Filter]],
) -> Optional[_filters.Filter]:
    if isinstance(experiments, str):
        return _filters.Filter.matches_all(_filters.Attribute("sys/name", type="string"), experiments)
    if isinstance(experiments, list):
        return _filters.Filter.name_in(*experiments)
    return experiments


def resolve_attributes_filter(
    attributes: Optional[Union[str, list[str], _filters.BaseAttributeFilter]],
    forced_type: Optional[list[_filters.ATTRIBUTE_LITERAL]] = None,
) -> _filters.BaseAttributeFilter:
    if forced_type is None:
        if attributes is None:
            return _filters.AttributeFilter()
        if isinstance(attributes, str):
            return _filters.AttributeFilter(name_matches_all=attributes)
        if isinstance(attributes, list):
            return _filters.AttributeFilter(name_eq=attributes)
        if isinstance(attributes, _filters.BaseAttributeFilter):
            return attributes
        raise ValueError(
            f"Invalid type for attributes: {type(attributes)}. Expected str, list, or BaseAttributeFilter."
        )
    else:
        if attributes is None:
            return _filters.AttributeFilter(type_in=forced_type)
        if isinstance(attributes, str):
            return _filters.AttributeFilter(name_matches_all=attributes, type_in=forced_type)
        if isinstance(attributes, list):
            return _filters.AttributeFilter(name_eq=attributes, type_in=forced_type)
        if isinstance(attributes, _filters.BaseAttributeFilter):
            # TODO: no forcing type here? Should this be limited somehow?
            #  For _AllAttributesFilter, should we iterate over all sub-attributes and force type on them?
            return attributes
        raise ValueError(
            f"Invalid type for attributes: {type(attributes)}. Expected str, list, or BaseAttributeFilter."
        )


def resolve_sort_by(sort_by: Union[str, _filters.Attribute]) -> _filters.Attribute:
    if isinstance(sort_by, str):
        return _filters.Attribute(sort_by)
    return sort_by


def resolve_destination_path(destination: Optional[str]) -> pathlib.Path:
    if destination is None:
        return pathlib.Path.cwd()
    else:
        return pathlib.Path(destination).resolve()
