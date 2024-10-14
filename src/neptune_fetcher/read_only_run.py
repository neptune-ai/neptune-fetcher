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
    Tuple,
    Union,
)

from neptune.exceptions import MetadataContainerNotFound
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
    from neptune.typing import ProgressBarType

    from neptune_fetcher.read_only_project import ReadOnlyProject


def get_attribute_value_from_entry(entry: TableEntry, name: str) -> Optional[str]:
    try:
        return entry.get_attribute_value(name)
    except ValueError:
        return None


class ReadOnlyRun:
    def __init__(
        self,
        read_only_project: "ReadOnlyProject",
        with_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        experiment_name: Optional[str] = None,
    ) -> None:
        self.project = read_only_project

        verify_type("with_id", with_id, (str, type(None)))
        verify_type("custom_id", custom_id, (str, type(None)))
        verify_type("experiment_name", experiment_name, (str, type(None)))

        if with_id is None and custom_id is None and experiment_name is None:
            raise ValueError("You must provide one of: `with_id`, `custom_id`, and `experiment_name`.")

        if sum([with_id is not None, custom_id is not None, experiment_name is not None]) != 1:
            raise ValueError("You must provide exactly one of: `with_id`, `custom_id`, and `experiment_name`.")

        if custom_id is not None:
            try:
                experiment = read_only_project._backend.get_metadata_container(
                    container_id=QualifiedName(f"CUSTOM/{read_only_project.project_identifier}/{custom_id}"),
                    expected_container_type=None,
                )
                self.with_id = experiment.sys_id
            except MetadataContainerNotFound as e:
                raise ValueError(f"No experiment found with custom id '{custom_id}'") from e
        elif experiment_name is not None:
            experiment = read_only_project.fetch_experiments_df(
                query=f"`sys/name`:string = '{experiment_name}'", limit=1, columns=["sys/id"]
            )
            if len(experiment) == 0:
                raise ValueError(f"No experiment found with name '{experiment_name}'")
            self.with_id = experiment.iloc[0]["sys/id"]
        else:
            try:
                # Just to check if the experiment exists
                _ = read_only_project._backend.get_metadata_container(
                    container_id=QualifiedName(f"{read_only_project.project_identifier}/{with_id}"),
                    expected_container_type=None,
                )
                self.with_id = with_id
            except MetadataContainerNotFound as e:
                raise ValueError(f"No experiment found with Neptune ID '{with_id}'") from e

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

    def prefetch_series_values(
        self,
        paths: List[str],
        use_threads: bool = False,
        progress_bar: "ProgressBarType" = None,
        include_inherited: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> None:
        """
        Prefetches values of a list of series and stores them in the local cache.

        Args:
            paths: List of field paths to prefetch.
            use_threads: If True, fetching is done concurrently.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.
            include_inherited: If `False`, values from the parent runs will not be included.
            step_range: tuple(left, right): Limits the range of steps to fetch. This must be a 2-tuple:
                - `left`: The left boundary of the range (exclusive). If `None`, the range is open on the left.
                - `right`: (currently not supported) The right boundary of the range (inclusive).
                            If `None`, the range is open on the right.
        To control the number of workers in the thread pool, set the
        NEPTUNE_FETCHER_MAX_WORKERS environment variable. The default value is 10.

        Example:
        ```
        run.prefetch_series_values(["metrics/loss", "metrics/accuracy"])

        # No more calls to the API
        print(run["metrics/loss"].fetch_values())
        print(run["metrics/accuracy"].fetch_values())
        ```
        """
        self._cache.prefetch_series_values(
            paths=paths,
            use_threads=use_threads,
            progress_bar=progress_bar,
            include_inherited=include_inherited,
            step_range=step_range,
        )
