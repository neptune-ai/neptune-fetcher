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
import pathlib
from typing import (
    Iterable,
    Optional,
    Union,
)

import pandas as pd

from neptune_query import filters
from neptune_query.exceptions import NeptuneProjectNotProvided
from neptune_query.internal import filters as _filters
from neptune_query.internal.context import get_context
from neptune_query.internal.identifiers import ProjectIdentifier
from neptune_query.types import File

# This module contains utility functions to resolve parameters to public functions from neptune_query
# and translates them to internal objects like _Filter and _Attribute that are used in the internal API.


def resolve_experiments_filter(
    experiments: Optional[Union[str, list[str], filters.Filter]],
) -> Optional[_filters._Filter]:
    if experiments is None:
        return None
    if isinstance(experiments, str):
        return filters.Filter.name(experiments)._to_internal()
    if experiments == []:
        raise ValueError(
            "Invalid type for `experiments` filter. Expected str, non-empty list of str, or Filter object, but got "
            "an empty list"
        )
    if isinstance(experiments, list):
        # Right now, there's no Filter.any() in the public API, so we use the internal _Filter.any()
        # (The reason for not having Filter.any() is that we cannot express TRUE/FALSE in NQL
        # which would be the equivalent of Filter.any with an empty list)
        return _filters._Filter.any(
            [
                _filters._Filter.eq(filters.Attribute("sys/name", type="string")._to_internal(), exp_name)
                for exp_name in experiments
            ]
        )
    if isinstance(experiments, filters.Filter):
        return experiments._to_internal()
    raise ValueError(
        "Invalid type for `experiments` filter. Expected str, non-empty list of str, or Filter object, but got "
        f"{type(experiments)}."
    )


def resolve_attributes_filter(
    attributes: Optional[Union[str, list[str], filters.AttributeFilter]],
) -> _filters._BaseAttributeFilter:
    if attributes is None:
        return filters.AttributeFilter()._to_internal()
    if isinstance(attributes, str):
        return filters.AttributeFilter(name=attributes)._to_internal()
    if isinstance(attributes, list):
        return filters.AttributeFilter(name=attributes)._to_internal()
    if isinstance(attributes, filters.BaseAttributeFilter):
        return attributes._to_internal()
    raise ValueError(
        "Invalid type for `attributes` filter. Expected str, non-empty list of str, or AttributeFilter object, "
        f"but got {type(attributes)}."
    )


def resolve_sort_by(sort_by: Union[str, filters.Attribute]) -> _filters._Attribute:
    if isinstance(sort_by, str):
        return filters.Attribute(sort_by)._to_internal()
    if isinstance(sort_by, filters.Attribute):
        return sort_by._to_internal()
    raise ValueError(f"Invalid type for `sort_by`. Expected str or Attribute object, but got {type(sort_by)}.")


def resolve_runs_filter(runs: Optional[Union[str, list[str], filters.Filter]]) -> Optional[_filters._Filter]:
    if isinstance(runs, str):
        return filters.Filter.matches(filters.Attribute("sys/custom_run_id", type="string"), runs)._to_internal()
    if runs == []:
        raise ValueError(
            "Invalid type for `runs` filter. Expected str, non-empty list of str, or Filter object, but got "
            "an empty list"
        )
    if isinstance(runs, list):
        return _filters._Filter.any(
            [_filters._Filter.eq(_filters._Attribute("sys/custom_run_id", type="string"), run) for run in runs]
        )
    if isinstance(runs, filters.Filter):
        return runs._to_internal()
    if runs is None:
        return None
    raise ValueError(
        f"Invalid type for `runs` filter. Expected str, non-empty list of str, or Filter object, but got {type(runs)}."
    )


def get_default_project_identifier(project: Optional[str] = None) -> ProjectIdentifier:
    """
    Pass through the project name from the argument if set, otherwise, get one from env.
    """
    if not project:
        project = get_context().project

    if not project:
        raise NeptuneProjectNotProvided()

    return ProjectIdentifier(project)


def resolve_files(
    files: Union[File, Iterable[File], pd.Series, pd.DataFrame],
) -> list[File]:
    """
    Resolves the input files to a list of File objects.
    If a DataFrame is provided, it is converted to a File.
    """
    if isinstance(files, File):
        return [files]
    elif isinstance(files, pd.Series):
        return [file for file in files if isinstance(file, File)]
    elif isinstance(files, pd.DataFrame):
        return [file for _, col in files.items() for file in col if isinstance(file, File)]
    elif isinstance(files, Iterable):
        return [file for file in files if isinstance(file, File)]
    else:
        raise ValueError(
            f"Invalid type for `files`. Expected File, DataFrame, Series, "
            f"or iterable of Files, but got {type(files)}."
        )


def resolve_destination_path(destination: Optional[Union[str, pathlib.Path]]) -> pathlib.Path:
    if destination is None:
        return pathlib.Path.cwd()
    elif isinstance(destination, pathlib.Path):
        return destination.resolve()
    else:
        return pathlib.Path(destination).resolve()
