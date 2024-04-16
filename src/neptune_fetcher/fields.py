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
from neptune.internal.container_type import ContainerType

if typing.TYPE_CHECKING:
    from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
    from pandas import DataFrame

T = TypeVar("T")


@dataclass
class Series(ABC, Generic[T]):
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

    def fetch_last(self) -> Optional[T]:
        return self.last


class FloatSeries(Series[float]):
    ...


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
