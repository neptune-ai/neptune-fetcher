__all__ = ("FieldsCache",)

import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import (
    Dict,
    List,
    Union,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils.paths import parse_path

from neptune_fetcher.fetchable import FieldToFetchableVisitor
from neptune_fetcher.fields import (
    Field,
    Series,
)


class FieldsCache(Dict[str, Union[Field, Series]]):
    def __init__(self, backend: NeptuneBackend, container_id: QualifiedName, container_type: ContainerType):
        super().__init__()
        self._backend: NeptuneBackend = backend
        self._container_id: QualifiedName = container_id
        self._container_type = container_type
        self._field_to_fetchable_visitor = FieldToFetchableVisitor()

        self._lock = threading.RLock()

    def cache_miss(self, paths: List[str]) -> None:
        missed_paths = [path for path in paths if path not in self]

        if not missed_paths:
            return None

        data = self._backend.get_fields_with_paths_filter(
            container_id=self._container_id,
            container_type=ContainerType.RUN,
            paths=missed_paths,
            use_proto=True,
        )
        fetched = {field.path: self._field_to_fetchable_visitor.visit(field) for field in data}
        self.update(fetched)

    def prefetch(self, paths: List[str]) -> None:
        self.cache_miss(paths)

    def prefetch_series_values(self, paths: List[str], use_threads: bool) -> None:
        paths = list(set(paths))

        self.cache_miss(paths)

        if use_threads:
            with ThreadPoolExecutor(10) as executor:
                executor.map(self._fetch_single_series_values, paths)

        else:
            for path in paths:
                self._fetch_single_series_values(path)

    def _fetch_single_series_values(self, path: str) -> None:
        if not isinstance(self[path], Series):
            return None

        data = fetch_series_values(
            getter=partial(
                self._backend.get_float_series_values,
                container_id=self._container_id,
                container_type=ContainerType.RUN,
                path=parse_path(path),
            ),
            path=path,
            progress_bar=None,  # TODO: handle progress bar in parallel
        )
        self[path].prefetched_data = list(data)

    def __getitem__(self, path: str) -> Union[Field, Series]:
        self.cache_miss(
            paths=[
                path,
            ]
        )
        return super().__getitem__(path)
