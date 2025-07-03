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

# This module contains utility functions to resolve parameters to public functions from neptune_fetcher.alpha
# and translates them to internal objects like _Filter and _Attribute that are used in the internal API.

import pathlib
from typing import (
    Optional,
    Union,
)

from neptune_fetcher.alpha import filters
from neptune_fetcher.exceptions import NeptuneProjectNotProvided
from neptune_fetcher.internal import filters as _filters
from neptune_fetcher.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.internal.identifiers import ProjectIdentifier


def resolve_experiments_filter(
    experiments: Optional[Union[str, list[str], filters.Filter]],
) -> Optional[_filters._Filter]:
    if isinstance(experiments, str):
        return filters.Filter.matches_all(filters.Attribute("sys/name", type="string"), experiments)._to_internal()
    if experiments == []:
        # In alpha, passing experiments=[] gives us un-filtered results
        # In v1, we're going to return no results or raise an error
        return None
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
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]],
) -> _filters._AttributeFilter:
    if attributes is None:
        return filters.AttributeFilter()._to_internal()
    if isinstance(attributes, str):
        return filters.AttributeFilter(name_matches_all=attributes)._to_internal()
    if attributes == []:
        # In alpha, passing attributes=[] gives us un-filtered results
        # In v1, we're going to return no results or raise an error
        return filters.AttributeFilter()._to_internal()
    if isinstance(attributes, list):
        return filters.AttributeFilter(name_eq=attributes)._to_internal()
    if isinstance(attributes, filters.BaseAttributeFilter):
        return attributes._to_internal()
    raise ValueError(
        "Invalid type for attributes filter. Expected str, list of str, or AttributeFilter object, but got "
        f"{type(attributes)}."
    )


def resolve_sort_by(sort_by: Union[str, filters.Attribute]) -> _filters._Attribute:
    if isinstance(sort_by, str):
        return filters.Attribute(sort_by)._to_internal()
    if isinstance(sort_by, filters.Attribute):
        return sort_by._to_internal()
    raise ValueError(f"Invalid type for `sort_by`. Expected str or Attribute object, but got {type(sort_by)}.")


def resolve_destination_path(destination: Optional[str]) -> pathlib.Path:
    if destination is None:
        return pathlib.Path.cwd()
    else:
        return pathlib.Path(destination).resolve()


def resolve_runs_filter(runs: Optional[Union[str, list[str], filters.Filter]]) -> Optional[_filters._Filter]:
    if isinstance(runs, str):
        return filters.Filter.matches_all(
            filters.Attribute("sys/custom_run_id", type="string"), regex=runs
        )._to_internal()
    if runs == []:
        # In alpha, passing runs=[] gives us un-filtered results
        # In v1, we're going to return no results or raise an error
        return None
    if isinstance(runs, list):
        return filters.Filter.any(
            *[filters.Filter.eq(filters.Attribute("sys/custom_run_id", type="string"), value=run) for run in runs]
        )._to_internal()
    if isinstance(runs, filters.Filter):
        return runs._to_internal()
    if runs is None:
        return None
    raise ValueError(
        f"Invalid type for `runs` filter. Expected str, list of str, or Filter object, but got {type(runs)}."
    )


def get_default_project_identifier(context: Optional[Context] = None) -> ProjectIdentifier:
    """
    Returns the default project name from the current context.
    If no context is set, it returns 'default'.
    """
    valid_context = validate_context(context or get_context(), validate_project=True)
    project = valid_context.project
    if not project:
        raise NeptuneProjectNotProvided()
    return ProjectIdentifier(project)
