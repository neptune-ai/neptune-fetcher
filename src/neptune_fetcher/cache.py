__all__ = ("FieldsCache",)

import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import (
    Callable,
    Dict,
    List,
    Tuple,
    Union,
)

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
from neptune_fetcher.util import (
    fetch_series_values,
    getenv_int,
)

# Maximum number of paths to fetch in a single request for fields definitions.
MAX_PATHS_PER_REQUEST = getenv_int("NEPTUNE_MAX_PATHS_PER_REQUEST", 8000)


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

        # Split paths into chunks to avoid hitting the server limit in a single request
        for start in range(0, len(missed_paths), MAX_PATHS_PER_REQUEST):
            end = start + MAX_PATHS_PER_REQUEST
            chunk = missed_paths[start:end]

            data = self._backend.get_fields_with_paths_filter(
                container_id=self._container_id,
                container_type=ContainerType.RUN,
                paths=chunk,
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
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> None:
        self.cache_miss(paths)

        with construct_progress_bar(progress_bar, description="Fetching metrics") as progress_bar:
            progress_bar.update(by=0, total=len(paths))
            if use_threads:
                fetch_values_concurrently(
                    partial(
                        self._fetch_single_series_values, include_inherited=include_inherited, step_range=step_range
                    ),
                    paths=paths,
                    progress_bar=progress_bar,
                )
            else:
                fetch_values_sequentially(
                    partial(
                        self._fetch_single_series_values, include_inherited=include_inherited, step_range=step_range
                    ),
                    paths=paths,
                    progress_bar=progress_bar,
                )

    def _fetch_single_series_values(
        self,
        path: str,
        progress_bar: ProgressBarCallback,
        include_inherited: bool,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
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
                from_step=step_range[0],
            )
        )
        self[path].include_inherited = include_inherited
        self[path].step_range = step_range
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
