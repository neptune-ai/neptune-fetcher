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
    "Series",
    "String",
    "Bool",
    "DateTime",
    "ObjectState",
    "StringSet",
]

import abc
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import (
    TYPE_CHECKING,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.api.models import (
    FieldType,
    FloatPointValue,
    FloatSeriesValues,
    StringPointValue,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.utils.paths import parse_path

if TYPE_CHECKING:
    from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
    from pandas import DataFrame

T = TypeVar("T")
Row = TypeVar("Row", StringPointValue, FloatPointValue)


def make_row(entry: Row, include_timestamp: bool = True) -> Dict[str, Union[str, float, datetime]]:
    row: Dict[str, Union[str, float, datetime]] = {
        "step": entry.step,
        "value": entry.value,
    }

    if include_timestamp:
        row["timestamp"] = entry.timestamp

    return row


@dataclass
class Series(ABC, Generic[T]):
    type: FieldType
    last: Optional[T] = None

    def fetch_values(
        self,
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: str,
        include_timestamp: bool = True,
    ) -> "DataFrame":
        import pandas as pd

        data = fetch_series_values(
            getter=partial(
                self._fetch_values_from_backend,
                backend=backend,
                container_id=container_id,
                container_type=container_type,
                path=parse_path(path),
            ),
            path=path,
            progress_bar=None,
        )

        rows = dict((n, make_row(entry=entry, include_timestamp=include_timestamp)) for (n, entry) in enumerate(data))
        return pd.DataFrame.from_dict(data=rows, orient="index")

    @abc.abstractmethod
    def _fetch_values_from_backend(
        self,
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
    ):
        ...

    def fetch_last(self) -> Optional[T]:
        return self.last


class FloatSeries(Series[float]):
    def _fetch_values_from_backend(
        self,
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: List[str],
        limit: int,
        from_step: Optional[float] = None,
    ) -> FloatSeriesValues:
        return backend.get_float_series_values(
            container_id=container_id,
            container_type=container_type,
            path=path,
            from_step=from_step,
            limit=limit,
        )


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
