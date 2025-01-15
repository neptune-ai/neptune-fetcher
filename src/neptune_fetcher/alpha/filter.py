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

from abc import ABC
from dataclasses import dataclass, field
from typing import (
    Literal,
    Optional,
    Union,
)

__ALL__ = (
    "AttributeFilter",
)


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return self.any(self, other)

    def any(*filters: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return _AttributeFilterAlternative(*filters)


@dataclass
class AttributeFilter(BaseAttributeFilter):
    name_eq: Union[str, list[str], None] = None
    type_in: Optional[list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]]] = \
        field(default_factory=lambda: ["float", "int", "string", "bool", "datetime", "float_series", "string_set"])
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: Optional[list[Literal["last", "min", "max", "average", "variance", "auto"]]] = None


@dataclass
class _AttributeFilterAlternative(BaseAttributeFilter):
    filters: list[AttributeFilter]
