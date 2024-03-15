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
    "which_fetchable",
    "Fetchable",
    "FetchableAtom",
    "FetchableSeries",
]

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Union,
)

from neptune.common.warnings import NeptuneUnsupportedType
from neptune.internal.backends.api_model import Attribute
from neptune.internal.container_type import ContainerType

from neptune_fetcher.attribute_type import AttributeType
from neptune_fetcher.attributes import (
    Attr,
    Float,
    FloatSeries,
    Integer,
    Series,
    String,
)

if TYPE_CHECKING:
    from pandas import DataFrame

    from neptune_fetcher.custom_backend import CustomBackend


ATOMS = {
    AttributeType.INT,
    AttributeType.FLOAT,
    AttributeType.STRING,
}
SERIES = {AttributeType.FLOAT_SERIES}


class Fetchable(ABC):
    def __init__(
        self,
        attribute: Attribute,
        backend: "CustomBackend",
        container_id: str,
        cache: Dict[str, Union[Attr, Series]],
    ) -> None:
        self._attribute = attribute
        self._backend = backend
        self._container_id = container_id
        self._cache = cache

    @abstractmethod
    def fetch(self):
        ...


class FetchableAtom(Fetchable):
    def fetch(self):
        if self._attribute.path in self._cache:
            return self._cache[self._attribute.path].val
        if self._attribute.type == AttributeType.STRING:
            attr = String(self._attribute.type)
        elif self._attribute.type == AttributeType.INT:
            attr = Integer(self._attribute.type)
        elif self._attribute.type == AttributeType.FLOAT:
            attr = Float(self._attribute.type)
        else:
            raise NeptuneUnsupportedType()
        attr.val = attr.fetch(self._backend, self._container_id, ContainerType.RUN, [self._attribute.path])
        self._cache[self._attribute.path] = attr
        return attr.val


class FetchableSeries(Fetchable):
    def fetch(self):
        raise NeptuneUnsupportedType()

    def fetch_values(self, *, include_timestamp: bool = True) -> "DataFrame":
        if self._attribute.path in self._cache:
            # from cache
            return self._cache[self._attribute.path].values
        if self._attribute.type == AttributeType.FLOAT_SERIES:
            series = FloatSeries()
        else:
            raise NeptuneUnsupportedType()
        series.values = series.fetch_values(
            self._backend, self._container_id, ContainerType.RUN, [self._attribute.path], include_timestamp
        )
        self._cache[self._attribute.path] = series
        return series.values


def which_fetchable(attribute: Attribute, *args: Any, **kwargs: Any) -> Fetchable:
    if attribute.type in ATOMS:
        return FetchableAtom(attribute, *args, **kwargs)
    elif attribute.type in SERIES:
        return FetchableSeries(attribute, *args, **kwargs)
    raise NeptuneUnsupportedType(f"Unsupported type: {attribute.type}")
