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
from __future__ import annotations

__all__ = [
    "which_fetchable",
    "Fetchable",
    "FetchableAtom",
    "FetchableSeries",
    "SUPPORTED_TYPES",
    "FieldToFetchableVisitor",
]

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
)

from neptune.api.models import (
    ArtifactField,
    BoolField,
    DateTimeField,
    FieldDefinition,
    FieldType,
    FieldVisitor,
    FileField,
    FileSetField,
    FloatField,
    FloatSeriesField,
    GitRefField,
    ImageSeriesField,
    IntField,
    NotebookRefField,
    ObjectStateField,
    StringField,
    StringSeriesField,
    StringSetField,
)
from neptune.internal.utils.logger import get_logger
from neptune.internal.warnings import NeptuneUnsupportedType

from neptune_fetcher.fields import (
    Field,
    Float,
    Integer,
    String,
)

if TYPE_CHECKING:
    from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
    from pandas import DataFrame

    from neptune_fetcher.cache import FieldsCache


logger = get_logger()


SUPPORTED_ATOMS = {
    FieldType.INT,
    FieldType.FLOAT,
    FieldType.STRING,
}
SUPPORTED_SERIES_TYPES = set()
SUPPORTED_TYPES = {*SUPPORTED_ATOMS, *SUPPORTED_SERIES_TYPES}


class Fetchable(ABC):
    def __init__(
        self,
        field: FieldDefinition,
        backend: "HostedNeptuneBackend",
        container_id: str,
        cache: FieldsCache,
    ) -> None:
        self._field = field
        self._backend = backend
        self._container_id = container_id
        self._cache: FieldsCache = cache

    @abstractmethod
    def fetch(self):
        ...


class NoopFetchable(Fetchable):
    def fetch(self) -> None:
        logger.info("Unsupported type: %s", self._field.type)
        return None


class FetchableAtom(Fetchable):
    def fetch(self):
        return self._cache[self._field.path].val


class FetchableSeries(Fetchable):
    def fetch(self):
        raise NeptuneUnsupportedType()

    def fetch_values(self, *, include_timestamp: bool = True) -> "DataFrame":
        raise NeptuneUnsupportedType()


def which_fetchable(field: FieldDefinition, *args: Any, **kwargs: Any) -> Fetchable:
    if field.type in SUPPORTED_ATOMS:
        return FetchableAtom(field, *args, **kwargs)
    elif field.type in SUPPORTED_SERIES_TYPES:
        return FetchableSeries(field, *args, **kwargs)
    return NoopFetchable(field, *args, **kwargs)


class FieldToFetchableVisitor(FieldVisitor[Field]):
    def visit_float(self, field: FloatField) -> Field:
        return Float(field.type, val=field.value)

    def visit_int(self, field: IntField) -> Field:
        return Integer(field.type, val=field.value)

    def visit_bool(self, field: BoolField) -> Field:
        raise NotImplementedError

    def visit_string(self, field: StringField) -> Field:
        return String(field.type, val=field.value)

    def visit_datetime(self, field: DateTimeField) -> Field:
        raise NotImplementedError

    def visit_file(self, field: FileField) -> Field:
        raise NotImplementedError

    def visit_file_set(self, field: FileSetField) -> Field:
        raise NotImplementedError

    def visit_float_series(self, field: FloatSeriesField) -> Field:
        raise NotImplementedError

    def visit_string_series(self, field: StringSeriesField) -> Field:
        raise NotImplementedError

    def visit_image_series(self, field: ImageSeriesField) -> Field:
        raise NotImplementedError

    def visit_string_set(self, field: StringSetField) -> Field:
        raise NotImplementedError

    def visit_git_ref(self, field: GitRefField) -> Field:
        raise NotImplementedError

    def visit_object_state(self, field: ObjectStateField) -> Field:
        raise NotImplementedError

    def visit_notebook_ref(self, field: NotebookRefField) -> Field:
        raise NotImplementedError

    def visit_artifact(self, field: ArtifactField) -> Field:
        raise NotImplementedError
