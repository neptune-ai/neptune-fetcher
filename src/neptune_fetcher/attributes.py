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
    "Attr",
    "Float",
    "FloatSeries",
    "Integer",
    "Series",
    "String",
]

import typing
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from neptune.attributes.atoms.float import Float as FloatAttr
from neptune.attributes.atoms.integer import Integer as IntegerAttr
from neptune.attributes.atoms.string import String as StringAttr
from neptune.attributes.series.fetchable_series import Row
from neptune.internal.backends.api_model import FloatSeriesValues
from neptune.internal.container_type import ContainerType

from neptune_fetcher.attribute_type import AttributeType

if typing.TYPE_CHECKING:
    from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
    from pandas import DataFrame

T = TypeVar("T")


@dataclass
class Series(ABC, Generic[T]):
    values: Optional["DataFrame"] = None
    last: Optional[T] = None

    def fetch_values(
        self,
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
        include_timestamp: bool = True,
    ) -> "DataFrame":
        import pandas as pd

        limit = 1000
        val = self._fetch_values_from_backend(backend, container_id, container_type, path, 0, limit)
        data = val.values
        offset = limit

        def make_row(entry: Row) -> Dict[str, Union[str, float, datetime]]:
            row: Dict[str, Union[str, float, datetime]] = dict()
            row["step"] = entry.step
            row["value"] = entry.value
            if include_timestamp:
                row["timestamp"] = datetime.fromtimestamp(entry.timestampMillis / 1000)
            return row

        while offset < val.totalItemCount:
            batch = self._fetch_values_from_backend(backend, container_id, container_type, path, offset, limit)
            data.extend(batch.values)
            offset += limit

        rows = dict((n, make_row(entry)) for (n, entry) in enumerate(data))

        df = pd.DataFrame.from_dict(data=rows, orient="index")
        return df

    @staticmethod
    def _fetch_values_from_backend(
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
        offset: int,
        limit: int,
    ) -> Row:
        ...

    @staticmethod
    def fetch_last(backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: str) -> T:
        ...


class FloatSeries(Series[float]):
    @staticmethod
    def _fetch_values_from_backend(
        backend: "HostedNeptuneBackend",
        container_id: str,
        container_type: ContainerType,
        path: typing.List[str],
        offset: int,
        limit: int,
    ) -> FloatSeriesValues:
        return backend.get_float_series_values(container_id, container_type, path, offset, limit)

    @staticmethod
    def fetch_last(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: str
    ) -> float:
        return backend.get_float_series_attribute(container_id, container_type, [path]).last


@dataclass
class Attr(Generic[T], ABC):
    type: AttributeType
    val: Optional[T] = None

    @staticmethod
    def fetch(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> T:
        ...


class Integer(Attr[int]):
    @staticmethod
    def fetch(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> int:
        return IntegerAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class Float(Attr[float]):
    @staticmethod
    def fetch(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> float:
        return FloatAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )


class String(Attr[str]):
    @staticmethod
    def fetch(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> str:
        return StringAttr.getter(
            backend=backend,
            container_id=container_id,
            container_type=container_type,
            path=path,
        )
