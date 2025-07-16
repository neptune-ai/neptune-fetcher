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
import dataclasses
import os
import pathlib
from typing import (
    Iterable,
    Literal,
    Optional,
    Tuple,
)

from .. import filters
from ..retrieval.attribute_types import ATTRIBUTE_LITERAL


def restrict_attribute_filter_type(
    attribute_filter: filters._BaseAttributeFilter,
    type_in: Iterable[ATTRIBUTE_LITERAL],
) -> filters._BaseAttributeFilter:
    def restrict_type(leaf: filters._AttributeFilter) -> filters._AttributeFilter:
        # user: [A], type_in: [A, B] => [A]  # it's ok for user to request less types
        # user: [A, B], type_in: [A] => [A]  # remove types that are not supported
        # user: [A], type_in: [B] => raise   # raise an error when there is a complete mismatch
        intersection = [t for t in leaf.type_in if t in type_in]
        if intersection:
            return dataclasses.replace(leaf, type_in=intersection)
        else:
            types = list(type_in)
            if len(types) == 1:
                label = f"{types[0]} type is"
            else:
                label = f"{', '.join(types[:-1])} or {types[-1]} types are"

            raise ValueError(
                f"Only {label} supported for attribute filters in this function "
                f"and the filter contains types: {', '.join(leaf.type_in)}"
            )

    return attribute_filter.transform(map_attribute_filter=restrict_type)


def validate_include_time(include_time: Optional[Literal["absolute"]]) -> None:
    if include_time is not None and include_time not in ["absolute"]:
        raise ValueError("include_time must be 'absolute'")


def validate_step_range(step_range: Tuple[Optional[float], Optional[float]]) -> None:
    """Validate that a step range tuple contains valid values and is properly ordered."""
    if not isinstance(step_range, tuple) or len(step_range) != 2:
        raise ValueError("step_range must be a tuple of two values")

    start, end = step_range

    # Validate types
    if start is not None and not isinstance(start, (int, float)):
        raise ValueError("step_range start must be None or a number")
    if end is not None and not isinstance(end, (int, float)):
        raise ValueError("step_range end must be None or a number")

    # Validate range order if both values are provided
    if start is not None and end is not None and start > end:
        raise ValueError("step_range start must be less than or equal to end")


def _validate_optional_positive_int(value: Optional[int], name: str) -> None:
    """Validate that value is either None or a positive integer, with a custom name for error messages."""
    if value is not None:
        if not isinstance(value, int):
            raise ValueError(f"{name} must be None or an integer")
        if value <= 0:
            raise ValueError(f"{name} must be greater than 0")


def validate_tail_limit(tail_limit: Optional[int]) -> None:
    """Validate that tail_limit is either None or a positive integer."""
    _validate_optional_positive_int(tail_limit, "tail_limit")


def validate_limit(limit: Optional[int]) -> None:
    """Validate that limit is either None or a positive integer."""
    _validate_optional_positive_int(limit, "limit")


def validate_sort_direction(sort_direction: Literal["asc", "desc"]) -> Literal["asc", "desc"]:
    """Validate that sort_direction is either 'asc' or 'desc'."""
    if sort_direction not in ("asc", "desc"):
        raise ValueError(f"sort_direction '{sort_direction}' is invalid; must be 'asc' or 'desc'")
    return sort_direction


def ensure_write_access(destination: pathlib.Path) -> None:
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    if not destination.is_dir():
        raise NotADirectoryError(f"Destination is not a directory: {destination}")

    if not os.access(destination, os.W_OK):
        raise PermissionError(f"No write access to the directory: {destination}")
