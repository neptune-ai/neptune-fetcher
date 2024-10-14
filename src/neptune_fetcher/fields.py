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
    Tuple,
    TypeVar,
    Union,
)

from neptune.api.fetching_series_values import PointValue
from neptune.api.models import (
    FieldType,
    FloatPointValue,
    FloatSeriesValues,
    StringPointValue,
)
from neptune.internal.container_type import ContainerType
from neptune.internal.utils.paths import parse_path
from neptune.typing import ProgressBarType

from neptune_fetcher.util import fetch_series_values

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
    include_inherited: bool = True
    last: Optional[T] = None
    prefetched_data: Optional[List[PointValue]] = None
    step_range = (None, None)

    def fetch_values(
        self,
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: str,
        include_timestamp: bool = True,
        include_inherited: bool = True,
        progress_bar: "ProgressBarType" = None,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> "DataFrame":
        import pandas as pd

        if self.prefetched_data is None or self.include_inherited != include_inherited or self.step_range != step_range:
            data = fetch_series_values(
                getter=partial(
                    self._fetch_values_from_backend,
                    backend=backend,
                    container_id=container_id,
                    container_type=container_type,
                    path=parse_path(path),
                    include_inherited=include_inherited,
                    from_step=step_range[0],
                )
            )
        else:
            data = self.prefetched_data

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
        include_inherited: bool = True,
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
        include_inherited: bool = True,
    ) -> FloatSeriesValues:
        return backend.get_float_series_values(
            container_id=container_id,
            container_type=container_type,
            path=path,
            from_step=from_step,
            limit=limit,
            include_inherited=include_inherited,
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
