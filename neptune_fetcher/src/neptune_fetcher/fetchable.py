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
]

import logging
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Tuple,
    Union,
)

from pandas import DataFrame

from neptune_fetcher.api.api_client import ApiClient
from neptune_fetcher.fields import (
    FieldDefinition,
    FieldType,
)

if TYPE_CHECKING:
    from neptune_fetcher.cache import FieldsCache

logger = logging.getLogger(__name__)

SUPPORTED_ATOMS = {
    FieldType.INT,
    FieldType.FLOAT,
    FieldType.STRING,
    FieldType.BOOL,
    FieldType.DATETIME,
    FieldType.STRING_SET,
    FieldType.OBJECT_STATE,
}
SUPPORTED_SERIES_TYPES = {
    FieldType.FLOAT_SERIES,
}
SUPPORTED_TYPES = {*SUPPORTED_ATOMS, *SUPPORTED_SERIES_TYPES}


class Fetchable(ABC):
    def __init__(
        self,
        field: FieldDefinition,
        backend: "ApiClient",
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


class UnsupportedFetchable(Fetchable):
    def fetch(self) -> None:
        return None

    # Make sure users expecting series of not supported types don't crash
    def fetch_last(self):
        return None

    def fetch_values(self, *args, **kwargs) -> DataFrame:
        return DataFrame()


class FetchableAtom(Fetchable):
    def fetch(self):
        """
        Retrieves a value either from the internal cache (see `prefetch()`) or from the API.
        """
        return self._cache[self._field.path].val


class FetchableSeries(Fetchable):
    def fetch(self):
        """
        Retrieves the last value of a series, either from the internal cache (see `prefetch`) or from the API.
        """
        return self._cache[self._field.path].last

    def fetch_last(self):
        """
        Retrieves the last value of a series, either from the internal cache (see `prefetch`) or from the API.
        """
        return self.fetch()

    def fetch_values(
        self,
        *,
        include_timestamp: bool = True,
        include_inherited: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
        progress_bar: bool = False,
        include_point_previews: bool = False,
    ) -> "DataFrame":
        """
        Retrieves all series values either from the internal cache (see `prefetch_series_values()`) or from the API.

        Args:
            include_timestamp: Whether the fetched data should include the timestamp field.
            include_inherited: Whether the fetched data should include values from the parent runs.
            step_range: tuple(left, right): Limits the range of steps to fetch. This must be a 2-tuple:
                - `left`: The left boundary of the range (exclusive). If `None`, the range is open on the left.
                - `right`: (currently not supported) The right boundary of the range (inclusive).
                            If `None`, the range is open on the right.
            progress_bar: Set to `False `to disable the download progress bar.
            include_point_previews - False by default. If False the returned results will only contain committed
                points. If True the results will also include preview points and the returned DataFrame will
                have additional sub-columns with preview status (is_preview and preview_completion).
        """
        return self._cache[self._field.path].fetch_values(
            backend=self._backend,
            container_id=self._container_id,
            path=self._field.path,
            include_timestamp=include_timestamp,
            include_inherited=include_inherited,
            progress_bar=progress_bar,
            step_range=step_range,
            include_point_previews=include_point_previews,
        )


def which_fetchable(field: FieldDefinition, *args: Any, **kwargs: Any) -> Fetchable:
    if field.type in SUPPORTED_ATOMS:
        return FetchableAtom(field, *args, **kwargs)
    elif field.type in SUPPORTED_SERIES_TYPES:
        return FetchableSeries(field, *args, **kwargs)
    return UnsupportedFetchable(field, *args, **kwargs)
