__all__ = ("FieldsCache",)

import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import (
    Callable,
    Dict,
    List,
    Union,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.utils import construct_progress_bar
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils.paths import parse_path
from neptune.typing import (
    ProgressBarCallback,
    ProgressBarType,
)

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

    def cache_miss(self, paths: List[str]) -> None:
        missed_paths = [path for path in paths if path not in self]

        if not missed_paths:
            return None

        missed_paths = list(set(missed_paths))

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

    def prefetch_series_values(
        self,
        paths: List[str],
        use_threads: bool,
        progress_bar: "ProgressBarType" = None,
        include_inherited: bool = True,
    ) -> None:
        self.cache_miss(paths)

        with construct_progress_bar(progress_bar, description="Fetching metrics") as progress_bar:
            progress_bar.update(by=0, total=len(paths))
            if use_threads:
                fetch_values_concurrently(
                    partial(self._fetch_single_series_values, include_inherited=include_inherited),
                    paths=paths,
                    progress_bar=progress_bar,
                )
            else:
                fetch_values_sequentially(
                    partial(self._fetch_single_series_values, include_inherited=include_inherited),
                    paths=paths,
                    progress_bar=progress_bar,
                )

    def _fetch_single_series_values(
        self,
        path: str,
        progress_bar: ProgressBarCallback,
        include_inherited: bool,
    ) -> None:
        if not isinstance(self[path], Series):
            return None

        data = fetch_series_values(
            getter=partial(
                self._backend.get_float_series_values,
                container_id=self._container_id,
                container_type=ContainerType.RUN,
                path=parse_path(path),
                include_inherited=include_inherited,
            ),
            path=path,
            progress_bar=False,
        )
        self[path].include_inherited = include_inherited
        self[path].prefetched_data = list(data)

        progress_bar.update(by=1)

    def __getitem__(self, path: str) -> Union[Field, Series]:
        self.cache_miss(
            paths=[
                path,
            ]
        )
        return super().__getitem__(path)


def fetch_values_sequentially(
    getter: Callable[[str, ProgressBarCallback], None],
    paths: List[str],
    progress_bar: ProgressBarCallback,
) -> None:
    for path in paths:
        getter(path, progress_bar)


def fetch_values_concurrently(
    getter: Callable[[str, ProgressBarCallback], None],
    paths: List[str],
    progress_bar: ProgressBarCallback,
) -> None:
    max_workers = int(os.getenv("NEPTUNE_FETCHER_MAX_WORKERS", 10))

    with ThreadPoolExecutor(max_workers) as executor:
        executor.map(partial(getter, progress_bar=progress_bar), paths)
