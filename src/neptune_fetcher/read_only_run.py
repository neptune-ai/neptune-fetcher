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
__all__ = ["ReadOnlyRun"]

from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

from neptune_fetcher.cache import FieldsCache
from neptune_fetcher.fetchable import (
    SUPPORTED_TYPES,
    Fetchable,
    FetchableSeries,
    which_fetchable,
)
from neptune_fetcher.fields import (
    FieldDefinition,
    FieldType,
)
from neptune_fetcher.util import escape_nql_criterion

if TYPE_CHECKING:
    from neptune_fetcher.read_only_project import ReadOnlyProject


class ReadOnlyRun:
    def __init__(
        self,
        read_only_project: "ReadOnlyProject",
        with_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        experiment_name: Optional[str] = None,
        eager_load_fields: bool = True,
    ) -> None:
        self.project = read_only_project

        if with_id is None and custom_id is None and experiment_name is None:
            raise ValueError("You must provide one of: `with_id`, `custom_id`, and `experiment_name`.")

        if sum([with_id is not None, custom_id is not None, experiment_name is not None]) != 1:
            raise ValueError("You must provide exactly one of: `with_id`, `custom_id`, and `experiment_name`.")

        if custom_id is not None:
            run = read_only_project.fetch_runs_df(
                query=f'`sys/custom_run_id`:string = "{escape_nql_criterion(custom_id)}"', limit=1, columns=["sys/id"]
            )

            if len(run) == 0:
                raise ValueError(f"No experiment found with custom id '{custom_id}'")
            self.with_id = run.iloc[0]["sys/id"]
        elif experiment_name is not None:
            experiment = read_only_project.fetch_experiments_df(
                query=f'`sys/name`:string = "{escape_nql_criterion(experiment_name)}"', limit=1, columns=["sys/id"]
            )
            if len(experiment) == 0:
                raise ValueError(f"No experiment found with name '{experiment_name}'")
            self.with_id = experiment.iloc[0]["sys/id"]
        else:
            run = read_only_project.fetch_runs_df(
                query=f'`sys/id`:string = "{escape_nql_criterion(with_id)}"', limit=1, columns=["sys/id"]
            )
            if len(run) == 0:
                raise ValueError(f"No experiment found with Neptune ID '{with_id}'")
            self.with_id = with_id

        self._container_id = f"{self.project.project_identifier}/{self.with_id}"
        self._cache = FieldsCache(
            backend=self.project._backend,
            container_id=self._container_id,
        )
        self._loaded_structure = False
        if eager_load_fields:
            self._load_structure()
        else:
            self._structure = {}

    def __getitem__(self, item: str) -> Union[Fetchable, FetchableSeries]:
        try:
            return self._structure[item]
        except KeyError:
            # If the item is not found, it could have been logged after the Run object is created.
            # We need to fetch it from the backend (this is what self._cache[item] actually does),
            # and fill in the missing structure entry. Note that self._cache[item] will also
            # raise KeyError if the field does not indeed exist backend-side.
            field = self._cache[item]
            self._structure[item] = which_fetchable(
                FieldDefinition(path=item, type=field.type), self.project._backend, self._container_id, self._cache
            )

            return self._structure[item]

    def __delitem__(self, key: str) -> None:
        del self._cache[key]

    @property
    def field_names(self) -> Generator[str, None, None]:
        """Lists names of run fields.

        Returns a generator of run fields.
        """
        self._load_structure()
        yield from self._structure

    def _load_structure(self):
        if not self._loaded_structure:
            definitions = self.project._backend.query_attribute_definitions(self._container_id)
            self._structure = {
                definition.path: which_fetchable(
                    definition,
                    self.project._backend,
                    self._container_id,
                    self._cache,
                )
                for definition in definitions
                if FieldType(definition.type) in SUPPORTED_TYPES
            }
            self._loaded_structure = True

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
        progress_bar: bool = True,
        include_inherited: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> None:
        """
        Prefetches values of a list of series and stores them in the local cache.
        This method skips the non-existing attributes.

        Args:
            paths: List of field paths to prefetch.
            use_threads: If True, fetching is done concurrently.
            progress_bar: Set to `False` to disable the download progress bar.
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
