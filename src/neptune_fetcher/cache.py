#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
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

__all__ = ("FieldsCache",)

import datetime
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Dict,
    Tuple,
    Union,
)

from neptune_api.api.retrieval import get_attributes_with_paths_filter_proto
from neptune_api.models import AttributeQueryDTO
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoAttributesDTO,
)
from tqdm import tqdm

from neptune_fetcher.api.api_client import ApiClient
from neptune_fetcher.fields import (
    Bool,
    DateTime,
    Field,
    FieldType,
    Float,
    FloatSeries,
    Integer,
    ObjectState,
    String,
    StringSet,
    Unsupported,
)
from neptune_fetcher.internal.retrieval.retry import handle_errors_default
from neptune_fetcher.util import (
    batched_paths,
    getenv_int,
    rethrow_neptune_error,
    warn_unsupported_value_type,
)

# Maximum number of paths to fetch in a single request for fields definitions.
MAX_PATHS_PER_REQUEST = getenv_int("NEPTUNE_MAX_PATHS_PER_REQUEST", 4096)
# Maximum sum of lengths of all the paths sent in a single request for fields definition
MAX_PATHS_SIZE_QUERY_LIMIT = getenv_int("NEPTUNE_MAX_PATH_SIZE_QUERY_LIMIT", 65536)

logger = logging.getLogger(__name__)


class FieldsCache(Dict[str, Union[Field, FloatSeries]]):
    def __init__(self, backend: ApiClient, container_id: str):
        super().__init__()
        self._backend: ApiClient = backend
        self._container_id: str = container_id

    def _fetch_missing_paths(self, paths: list[str]) -> None:
        missed_paths = [path for path in paths if path not in self]

        if not missed_paths:
            return

        missed_paths = list(set(missed_paths))

        # Split paths into chunks to avoid hitting the server limit in a single request
        for batch in batched_paths(missed_paths, MAX_PATHS_PER_REQUEST, MAX_PATHS_SIZE_QUERY_LIMIT):
            response = rethrow_neptune_error(
                handle_errors_default(get_attributes_with_paths_filter_proto.sync_detailed)
            )(
                client=self._backend._backend,
                body=AttributeQueryDTO.from_dict({"attributePathsFilter": batch}),
                holder_type="experiment",
                holder_identifier=self._container_id,
            )
            data: ProtoAttributesDTO = ProtoAttributesDTO.FromString(response.content)

            fetched = {attr.name: _extract_value(attr) for attr in data.attributes}
            self.update(fetched)

    def prefetch(self, paths: list[str]) -> None:
        self._fetch_missing_paths(paths)

    def prefetch_series_values(
        self,
        paths: list[str],
        use_threads: bool,
        progress_bar: bool = False,
        include_inherited: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> None:
        self._fetch_missing_paths(paths)

        float_series_paths = [path for path in paths if path in self and isinstance(self[path], FloatSeries)]

        max_workers = int(os.getenv("NEPTUNE_FETCHER_MAX_WORKERS", 10))

        with tqdm(
            desc="Fetching metrics", total=len(float_series_paths), unit="metrics", disable=not progress_bar
        ) as progress_bar, ThreadPoolExecutor(max_workers) as executor:
            lock = threading.Lock()
            batch_size = 300

            def fetch(start_index: int):
                result = self._backend.fetch_multiple_series_values(
                    float_series_paths[start_index : start_index + batch_size],
                    include_inherited=include_inherited,
                    container_id=self._container_id,
                    step_range=step_range,
                    include_point_previews=True,
                )

                for path, points in result:
                    points = list(points)
                    with lock:  # lock is inside the loop because result is a generator that fetches data lazily
                        self[path].include_inherited = include_inherited
                        self[path].step_range = step_range
                        self[path].prefetched_data = points
                        progress_bar.update()

            futures = executor.map(fetch, range(0, len(float_series_paths), batch_size))

            # Wait for all futures to finish
            list(futures)

    def __getitem__(self, path: str) -> Union[Field, FloatSeries]:
        self._fetch_missing_paths(
            paths=[
                path,
            ]
        )
        return super().__getitem__(path)


def _extract_value(attr: ProtoAttributeDTO) -> Union[Field, FloatSeries]:
    if attr.type == "floatSeries":
        return FloatSeries(FieldType.FLOAT_SERIES, last=attr.float_series_properties.last)
    elif attr.type == "string":
        return String(FieldType.STRING, attr.string_properties.value)
    elif attr.type == "int":
        return Integer(FieldType.INT, attr.int_properties.value)
    elif attr.type == "float":
        return Float(FieldType.FLOAT, attr.float_properties.value)
    elif attr.type == "bool":
        return Bool(FieldType.BOOL, attr.bool_properties.value)
    elif attr.type == "datetime":
        timestamp = datetime.datetime.fromtimestamp(attr.datetime_properties.value / 1000, tz=datetime.timezone.utc)
        return DateTime(FieldType.DATETIME, timestamp)
    elif attr.type == "stringSet":
        return StringSet(FieldType.STRING_SET, set(attr.string_set_properties.value))
    elif attr.type == "experimentState":
        return ObjectState(FieldType.OBJECT_STATE, "experiment_state")
    else:
        warn_unsupported_value_type(attr.type)
        return Unsupported(FieldType.UNSUPPORTED, None)
