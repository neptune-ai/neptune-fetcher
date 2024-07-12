__all__ = ("FieldsCache",)

import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import TracebackType
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
)

from neptune.api.fetching_series_values import fetch_series_values
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import QualifiedName
from neptune.internal.utils.paths import parse_path
from neptune.typing import ProgressBarCallback

from neptune_fetcher.fetchable import FieldToFetchableVisitor
from neptune_fetcher.fields import (
    Field,
    Series,
)


class ClickProgressBar(ProgressBarCallback):
    ...

    def __init__(self, *, description: Optional[str] = None, **_) -> None:
        ...
        super().__init__()

        from click import progressbar

        self._progress_bar = progressbar(iterable=None, length=1, label=description)

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        if total:
            self._progress_bar.length = total
        self._progress_bar.update(by)

    def __enter__(self) -> "ClickProgressBar":
        self._progress_bar.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:

        self._progress_bar.__exit__(exc_type, exc_val, exc_tb)


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

    def prefetch_series_values(self, paths: List[str], use_threads: bool) -> None:
        self.cache_miss(paths)

        if use_threads:
            fetch_values_concurrently(self._fetch_single_series_values, paths)
        else:
            fetch_values_sequentially(self._fetch_single_series_values, paths)

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
            progress_bar=ClickProgressBar,
        )
        self[path].prefetched_data = list(data)

    def __getitem__(self, path: str) -> Union[Field, Series]:
        self.cache_miss(
            paths=[
                path,
            ]
        )
        return super().__getitem__(path)


def fetch_values_sequentially(getter: Callable[[str], None], paths: List[str]) -> None:
    for path in paths:
        getter(path)


def fetch_values_concurrently(getter: Callable[[str], None], paths: List[str], *args, **kwargs) -> None:
    max_workers = kwargs.pop("max_workers", 10)
    with ThreadPoolExecutor(max_workers, *args, **kwargs) as executor:
        executor.map(getter, paths)
