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
    Collection,
    Optional,
    Sequence,
    Union,
)


def _validate_string_or_string_list(value: Optional[Union[str, list[str]]], field_name: str) -> None:
    """Validate that a value is either None, a string, or a list of strings."""
    if value is not None:
        if isinstance(value, list):
            if not all(isinstance(item, str) for item in value):
                raise ValueError(f"{field_name} must be a string or list of strings")
        elif not isinstance(value, str):
            raise ValueError(f"{field_name} must be a string or list of strings")


def _validate_string_list(value: Optional[list[str]], field_name: str) -> None:
    """Validate that a value is either None or a list of strings."""
    if value is not None:
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"{field_name} must be a list of strings")


def _validate_list_of_allowed_values(value: Sequence[str], allowed_values: Collection[str], field_name: str) -> None:
    """Validate that a value is a list containing only allowed values."""
    if not isinstance(value, Sequence) or not all(isinstance(v, str) and v in allowed_values for v in value):
        raise ValueError(f"{field_name} must be a list of valid values: {sorted(allowed_values)}")


def _validate_allowed_value(value: Optional[str], allowed_values: Collection[str], field_name: str) -> None:
    """Validate that a value is either None or one of the allowed values."""
    if value is not None and (not isinstance(value, str) or value not in allowed_values):
        raise ValueError(f"{field_name} must be one of: {sorted(allowed_values)}")
