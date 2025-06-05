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

# This module contains utility functions to resolve parameters to public functions from neptune_fetcher.v1
# and translates them to internal objects like _Filter and _Attribute that are used in the internal API.

import pathlib
from typing import (
    Optional,
    Union,
)

from neptune_fetcher.internal import filters as _filters
from neptune_fetcher.internal import pattern as _pattern
from neptune_fetcher.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.internal.identifiers import ProjectIdentifier
from neptune_fetcher.internal.retrieval import attribute_types as types
from neptune_fetcher.v1 import filters


def resolve_experiments_filter(
    experiments: Optional[Union[str, list[str], filters.Filter]],
) -> Optional[_filters._Filter]:
    if isinstance(experiments, str):
        return filters.Filter.name(experiments)._to_internal()
    if isinstance(experiments, list):
        return filters.Filter.name_in(*experiments)._to_internal()
    if isinstance(experiments, filters.Filter):
        return experiments._to_internal()
    if experiments is None:
        return None
    raise ValueError(
        "Invalid type for experiments filter. Expected str, list of str, or Filter object, but got "
        f"{type(experiments)}."
    )


def resolve_attributes_filter(
    # TODO: this function also accepts filters._AlternateAttributeFilter, but this is not fully tested...
    # see test_list_attributes_with_attribute_filter with "Combined filters" input
    # Note: it will fail for an Alternative if forced_type is provided...
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]],
    forced_type: Optional[list[filters.ATTRIBUTE_LITERAL]] = None,
) -> _filters._AttributeFilter:
    if forced_type is None:
        if attributes is None:
            return filters.AttributeFilter()._to_internal()
        if isinstance(attributes, str):
            return _pattern.build_extended_regex_attribute_filter(
                attributes, type_in=list(types.ALL_TYPES)  # type: ignore
            )
        if isinstance(attributes, list):
            return filters.AttributeFilter(name_eq=attributes)._to_internal()
        if isinstance(attributes, filters.BaseAttributeFilter):
            return attributes._to_internal()
        raise ValueError(
            "Invalid type for attributes filter. Expected str, list of str, or AttributeFilter object, but got "
            f"{type(attributes)}."
        )
    else:
        if attributes is None:
            return filters.AttributeFilter(type_in=forced_type)._to_internal()
        if isinstance(attributes, str):
            return _pattern.build_extended_regex_attribute_filter(attributes, type_in=forced_type)
        if isinstance(attributes, list):
            return filters.AttributeFilter(name_eq=attributes, type_in=forced_type)._to_internal()
        if isinstance(attributes, filters.AttributeFilter):
            modified_attributes = filters.AttributeFilter(
                name_eq=attributes.name_eq,
                name_matches_all=attributes.name_matches_all,
                name_matches_none=attributes.name_matches_none,
                type_in=forced_type,
                aggregations=attributes.aggregations,
            )
            return modified_attributes._to_internal()
        raise ValueError(
            "Invalid type for attributes filter. Expected str, list of str, or AttributeFilter object, but got "
            f"{type(attributes)}."
        )


def resolve_sort_by(sort_by: Union[str, filters.Attribute]) -> _filters._Attribute:
    if isinstance(sort_by, str):
        return _filters._Attribute(sort_by)
    if isinstance(sort_by, filters.Attribute):
        return sort_by._to_internal()
    raise ValueError(f"Invalid type for sort_by. Expected str or Attribute object, but got {type(sort_by)}.")


def resolve_destination_path(destination: Optional[str]) -> pathlib.Path:
    if destination is None:
        return pathlib.Path.cwd()
    else:
        return pathlib.Path(destination).resolve()


def resolve_runs_filter(runs: Optional[Union[str, list[str], filters.Filter]]) -> Optional[_filters._Filter]:
    if isinstance(runs, str):
        return filters.Filter.matches(
            filters.Attribute("sys/custom_run_id", type="string"), pattern=runs
        )._to_internal()
    if isinstance(runs, list):
        return filters.Filter.any(
            *[filters.Filter.eq(filters.Attribute("sys/custom_run_id", type="string"), value=run) for run in runs]
        )._to_internal()
    if isinstance(runs, filters.Filter):
        return runs._to_internal()
    if runs is None:
        return None
    raise ValueError(
        f"Invalid type for `runs` filter. Expected str, list[str], or Filter object, but got {type(runs)}."
    )


def get_default_project_identifier(context: Optional[Context] = None) -> ProjectIdentifier:
    """
    Returns the default project name from the current context.
    If no context is set, it returns 'default'.
    """
    valid_context = validate_context(context or get_context())
    project = valid_context.project
    if not project:
        raise ValueError("No project is set in the context. Please set a project before calling this function.")
    return ProjectIdentifier(project)
