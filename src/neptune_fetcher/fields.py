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
]

import typing
from abc import ABC
from dataclasses import dataclass
from typing import (
    Generic,
    Optional,
    TypeVar,
)

from neptune.api.models import FieldType
from neptune.attributes.atoms.float import Float as FloatAttr
from neptune.attributes.atoms.integer import Integer as IntegerAttr
from neptune.attributes.atoms.string import String as StringAttr
from neptune.attributes.series.fetchable_series import Row
from neptune.internal.backends.api_model import FloatSeriesValues
from neptune.internal.container_type import ContainerType

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
        raise NotImplementedError

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
class Field(Generic[T], ABC):
    type: FieldType
    val: Optional[T] = None

    @staticmethod
    def fetch(
        backend: "HostedNeptuneBackend", container_id: str, container_type: ContainerType, path: typing.List[str]
    ) -> T:
        ...


class Integer(Field[int]):
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


class Float(Field[float]):
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


class String(Field[str]):
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
