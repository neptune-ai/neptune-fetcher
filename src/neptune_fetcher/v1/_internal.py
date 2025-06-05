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

from typing import (
    Optional,
    Union,
)

from neptune_fetcher.internal import filters as _filters
from neptune_fetcher.internal.context import get_context
from neptune_fetcher.internal.identifiers import ProjectIdentifier
from neptune_fetcher.v1 import filters


def resolve_experiments_filter(
    experiments: Optional[Union[str, list[str]]],
    where: Optional[Union[filters.Filter, list[filters.Filter]]],
) -> Optional[_filters._Filter]:
    fs = []

    if isinstance(experiments, str):
        fs.append(_filters._Filter.matches_all(_filters._Attribute("sys/name", type="string"), experiments))
    elif isinstance(experiments, list):
        fs.append(_filters._Filter.name_in(*experiments))
    elif experiments is None:
        pass
    else:
        raise ValueError(
            "Invalid type for `experiments` filter. Expected str, list of str, but got " f"{type(experiments)}."
        )

    if isinstance(where, filters.Filter):
        fs.append(where._to_internal())
    elif isinstance(where, list):
        for w in where:
            if isinstance(w, filters.Filter):
                fs.append(w._to_internal())
            else:
                raise ValueError(
                    "Invalid type for `where` filter. Expected Filter object or list of Filter objects, but got "
                    f"{type(w)}."
                )
    elif where is None:
        pass
    else:
        raise ValueError(
            "Invalid type for `where` filter. Expected Filter object or list of Filter objects, but got "
            f"{type(where)}."
        )

    if len(fs) == 0:
        return None

    return _filters._Filter.all(*fs)


def resolve_attributes_filter(
    # TODO: this function also accepts filters._AlternateAttributeFilter, but this is not fully tested...
    # see test_list_attributes_with_attribute_filter with "Combined filters" input
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]],
    forced_type: Optional[list[filters.ATTRIBUTE_LITERAL]] = None,
) -> _filters._AttributeFilter:
    if forced_type is None:
        if attributes is None:
            return _filters._AttributeFilter()
        if isinstance(attributes, str):
            return _filters._AttributeFilter(name_matches_all=attributes)
        if isinstance(attributes, list):
            return _filters._AttributeFilter(name_eq=attributes)
        if isinstance(attributes, filters.BaseAttributeFilter):
            return attributes._to_internal()
        raise ValueError(
            "Invalid type for `attributes` filter. Expected str, list of str, or AttributeFilter object, but got "
            f"{type(attributes)}."
        )
    else:
        if attributes is None:
            return _filters._AttributeFilter(type_in=forced_type)
        if isinstance(attributes, str):
            return _filters._AttributeFilter(name_matches_all=attributes, type_in=forced_type)
        if isinstance(attributes, list):
            return _filters._AttributeFilter(name_eq=attributes, type_in=forced_type)
        if isinstance(attributes, filters.AttributeFilter):
            modified_attributes = filters.AttributeFilter(
                name_eq=attributes.name_eq,
                name_matches_all=attributes.name_matches_all,
                type_in=forced_type,
                aggregations=attributes.aggregations,
            )
            return modified_attributes._to_internal()
        raise ValueError(
            "Invalid type for `attributes` filter. Expected str, list of str, or AttributeFilter object, but got "
            f"{type(attributes)}."
        )


def resolve_sort_by(sort_by: Union[str, filters.Attribute]) -> _filters._Attribute:
    if isinstance(sort_by, str):
        return _filters._Attribute(sort_by)
    if isinstance(sort_by, filters.Attribute):
        return sort_by._to_internal()
    raise ValueError(f"Invalid type for sort_by. Expected str or Attribute object, but got {type(sort_by)}.")


def resolve_runs_filter(
    runs: Optional[Union[str, list[str]]],
    where: Optional[Union[filters.Filter, list[filters.Filter]]],
) -> Optional[_filters._Filter]:
    fs = []
    if isinstance(runs, str):
        fs.append(_filters._Filter.matches_all(_filters._Attribute("sys/custom_run_id", type="string"), regex=runs))
    elif isinstance(runs, list):
        fs.append(
            _filters._Filter.any(
                *[
                    _filters._Filter.eq(_filters._Attribute("sys/custom_run_id", type="string"), value=run)
                    for run in runs
                ]
            )
        )
    elif runs is None:
        pass
    else:
        raise ValueError(f"Invalid type for `runs` filter. Expected str, list[str], but got {type(runs)}.")

    if isinstance(where, filters.Filter):
        fs.append(where._to_internal())
    elif isinstance(where, list):
        for w in where:
            if isinstance(w, filters.Filter):
                fs.append(w._to_internal())
            else:
                raise ValueError(
                    "Invalid type for `where` filter. "
                    f"Expected Filter object or list of Filter objects, but got {type(w)}."
                )
    elif where is None:
        pass
    else:
        raise ValueError(
            f"Invalid type for `where` filter. Expected Filter object or list of Filter objects, but got {type(where)}."
        )

    if len(fs) == 0:
        return None

    return _filters._Filter.all(*fs)


def get_default_project_identifier(project: str = None) -> ProjectIdentifier:
    """
    Pass through the project name from the argument if set, otherwise, get one from env.
    """
    if not project:
        project = get_context().project

    if not project:
        raise ValueError("No project is set in the context. Please set a project before calling this function.")

    return ProjectIdentifier(project)
