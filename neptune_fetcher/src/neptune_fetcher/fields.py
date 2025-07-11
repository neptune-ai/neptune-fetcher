#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
__all__ = [
    "Field",
    "Float",
    "FloatSeries",
    "Integer",
    "String",
    "Bool",
    "DateTime",
    "ObjectState",
    "StringSet",
    "FieldType",
    "FieldDefinition",
    "FloatPointValue",
]

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from neptune_fetcher.util import warn_unsupported_value_type

if TYPE_CHECKING:
    from pandas import DataFrame

    from neptune_fetcher.api.api_client import ApiClient

T = TypeVar("T")


class FieldType(Enum):
    FLOAT = "float"
    INT = "int"
    BOOL = "bool"
    STRING = "string"
    DATETIME = "datetime"
    FLOAT_SERIES = "floatSeries"
    STRING_SET = "stringSet"
    OBJECT_STATE = "experimentState"

    UNSUPPORTED = "__unsupported_attribute_type__"

    @classmethod
    def _missing_(cls, value):
        warn_unsupported_value_type(value)
        return cls.UNSUPPORTED


@dataclass
class FieldDefinition:
    path: str
    type: FieldType


@dataclass
class FloatPointValue:
    timestamp: datetime
    value: float
    step: float
    preview: bool
    completion_ratio: float


@dataclass
class FloatSeries:
    type: FieldType
    include_inherited: bool = True
    last: Optional[T] = None
    prefetched_data: Optional[List[FloatPointValue]] = None
    step_range = (None, None)

    def fetch_values(
        self,
        backend: "ApiClient",
        container_id: str,
        path: str,
        include_timestamp: bool = True,
        include_inherited: bool = True,
        progress_bar: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
        include_point_previews: bool = False,
    ) -> "DataFrame":
        import pandas as pd

        if self.prefetched_data is None or self.include_inherited != include_inherited or self.step_range != step_range:
            data = backend.fetch_series_values(
                path=path,
                container_id=container_id,
                include_inherited=include_inherited,
                step_range=step_range,
                include_point_previews=include_point_previews,
            )

        else:
            data = self.prefetched_data

        # The motivation for this is to filter-out preview points downloaded during prefetching
        # in case the user does not request them.
        filtered_data = (entry for entry in data if include_point_previews or not entry.preview)

        rows = dict(
            (
                n,
                make_row(
                    entry=entry, include_timestamp=include_timestamp, include_point_previews=include_point_previews
                ),
            )
            for (n, entry) in enumerate(filtered_data)
        )
        return pd.DataFrame.from_dict(data=rows, orient="index")

    def fetch_last(self) -> Optional[T]:
        return self.last


@dataclass
class Field(Generic[T], ABC):
    type: FieldType
    val: T

    def fetch(self) -> T:
        return self.val


class Integer(Field[int]):
    ...


class Float(Field[float]):
    ...


class String(Field[str]):
    ...


class Bool(Field[bool]):
    ...


class DateTime(Field[datetime]):
    ...


class ObjectState(Field[str]):
    ...


class StringSet(Field[Set[str]]):
    ...


class Unsupported(Field[None]):
    def fetch(self) -> None:
        return None


def make_row(
    entry: FloatPointValue, include_timestamp: bool = True, include_point_previews: bool = False
) -> Dict[str, Union[str, float, datetime]]:
    row: Dict[str, Union[str, float, datetime]] = {
        "step": entry.step,
        "value": entry.value,
    }

    if include_timestamp:
        row["timestamp"] = entry.timestamp

    if include_point_previews:
        row["preview"] = entry.preview
        row["completion_ratio"] = entry.completion_ratio

    return row
