__all__ = ("FieldsCache",)

import datetime
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Dict,
    List,
    Tuple,
    Union,
)

from neptune_retrieval_api.api.default import get_attributes_with_paths_filter_proto
from neptune_retrieval_api.models import AttributeQueryDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
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
)
from neptune_fetcher.util import getenv_int

# Maximum number of paths to fetch in a single request for fields definitions.
MAX_PATHS_PER_REQUEST = getenv_int("NEPTUNE_MAX_PATHS_PER_REQUEST", 8000)

logger = logging.getLogger(__name__)


class FieldsCache(Dict[str, Union[Field, FloatSeries]]):
    def __init__(self, backend: ApiClient, container_id: str):
        super().__init__()
        self._backend: ApiClient = backend
        self._container_id: str = container_id

    def cache_miss(self, paths: List[str]) -> None:
        missed_paths = [path for path in paths if path not in self]

        if not missed_paths:
            return None

        missed_paths = list(set(missed_paths))

        # Split paths into chunks to avoid hitting the server limit in a single request
        for start in range(0, len(missed_paths), MAX_PATHS_PER_REQUEST):
            end = start + MAX_PATHS_PER_REQUEST
            chunk = missed_paths[start:end]

            response = get_attributes_with_paths_filter_proto.sync_detailed(
                client=self._backend._backend,
                body=AttributeQueryDTO.from_dict({"attributePathsFilter": chunk}),
                holder_type="experiment",
                holder_identifier=self._container_id,
            )
            data: ProtoAttributesDTO = ProtoAttributesDTO.FromString(response.content)

            fetched = {attr.name: _extract_value(attr) for attr in data.attributes}
            self.update(fetched)

    def prefetch(self, paths: List[str]) -> None:
        self.cache_miss(paths)

    def prefetch_series_values(
        self,
        paths: List[str],
        use_threads: bool,
        progress_bar: bool = False,
        include_inherited: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> None:
        self.cache_miss(paths)

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
        self.cache_miss(
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
        raise ValueError(f"Unsupported attribute type: {attr.type}, please update the neptune-fetcher")
