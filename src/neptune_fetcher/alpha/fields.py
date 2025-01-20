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


@dataclass
class FieldDefinition:
    path: str
    type: FieldType


@dataclass
class FloatPointValue:
    timestamp: datetime
    value: float
    step: float


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
    ) -> "DataFrame":
        import pandas as pd

        if self.prefetched_data is None or self.include_inherited != include_inherited or self.step_range != step_range:
            data = backend.fetch_series_values(
                path=path, container_id=container_id, include_inherited=include_inherited, step_range=step_range
            )

        else:
            data = self.prefetched_data

        rows = dict((n, make_row(entry=entry, include_timestamp=include_timestamp)) for (n, entry) in enumerate(data))
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


def make_row(entry: FloatPointValue, include_timestamp: bool = True) -> Dict[str, Union[str, float, datetime]]:
    row: Dict[str, Union[str, float, datetime]] = {
        "step": entry.step,
        "value": entry.value,
    }

    if include_timestamp:
        row["timestamp"] = entry.timestamp

    return row
