#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
    "ReadOnlyRun",
    "get_attribute_value_from_entry",
]

from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Optional,
    Union,
)

from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils import verify_type
from neptune.table import TableEntry

from neptune_fetcher.cache import FieldsCache
from neptune_fetcher.fetchable import (
    SUPPORTED_TYPES,
    Fetchable,
    FetchableSeries,
    which_fetchable,
)

if TYPE_CHECKING:
    from neptune_fetcher.read_only_project import ReadOnlyProject


def get_attribute_value_from_entry(entry: TableEntry, name: str) -> Optional[str]:
    try:
        return entry.get_attribute_value(name)
    except ValueError:
        return None


class ReadOnlyRun:
    def __init__(
        self, read_only_project: "ReadOnlyProject", with_id: Optional[str] = None, custom_id: Optional[str] = None
    ) -> None:
        self.project = read_only_project

        verify_type("with_id", with_id, (str, type(None)))
        verify_type("custom_id", custom_id, (str, type(None)))

        if with_id is None and custom_id is None:
            raise ValueError("Either `with_id` or `custom_id` must be provided.")

        if custom_id is not None:
            experiment = read_only_project._backend.get_metadata_container(
                container_id=QualifiedName(f"CUSTOM/{read_only_project.project_identifier}/{custom_id}"),
                expected_container_type=None,
            )
            self.with_id = experiment.sys_id
        else:
            self.with_id = with_id

        self._container_id = QualifiedName(f"{self.project.project_identifier}/{self.with_id}")
        self._cache = FieldsCache(
            backend=self.project._backend,
            container_id=self._container_id,
            container_type=ContainerType.RUN,
        )
        self._structure = {
            field_definition.path: which_fetchable(
                field_definition,
                self.project._backend,
                self._container_id,
                self._cache,
            )
            for field_definition in self.project._backend.get_fields_definitions(
                container_id=self._container_id,
                container_type=ContainerType.RUN,
                use_proto=True,
            )
            if field_definition.type in SUPPORTED_TYPES
        }

    def __getitem__(self, item: str) -> Union[Fetchable, FetchableSeries]:
        return self._structure[item]

    def __delitem__(self, key: str) -> None:
        del self._cache[key]

    @property
    def field_names(self) -> Generator[str, None, None]:
        """Lists names of run fields.

        Returns a generator of run fields.
        """
        yield from self._structure

    def prefetch(self, paths: List[str]) -> None:
        """Prefetches values of a list of fields and stores them in local cache.

        Args:
            paths: List of field paths to prefetch.
        """
        self._cache.prefetch(paths=paths)
