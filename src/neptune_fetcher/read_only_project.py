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
    "ReadOnlyProject",
]

import os
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Union,
)

from neptune.envs import (
    NEPTUNE_FETCH_TABLE_STEP_SIZE,
    PROJECT_ENV_NAME,
)
from neptune.internal.backends.api_model import Project
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.nql import (
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQueryAttribute,
)
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
    conform_optional,
)
from neptune.management.internal.utils import normalize_project_name
from neptune.objects.utils import prepare_nql_query
from neptune.table import Table
from neptune.typing import ProgressBarType

from neptune_fetcher.read_only_run import (
    ReadOnlyRun,
    get_attribute_value_from_entry,
)

if TYPE_CHECKING:
    from pandas import DataFrame


class ReadOnlyProject:
    """Class for retrieving metadata from a neptune.ai project in a limited read-only mode."""

    def __init__(
        self,
        project: Optional[str] = None,
        api_token: Optional[str] = None,
        proxies: Optional[dict] = None,
    ) -> None:
        """Initializes a Neptune project in limited read-only mode.

        Compared to a regular Project object, it contains only basic project information and exposes more lightweight
        methods for fetching run metadata.

        Args:
            project: The name of the Neptune project.
            api_token: Neptune account's API token.
                If left empty, the value of the NEPTUNE_API_TOKEN environment variable is used (recommended).
            proxies: A dictionary of proxy settings if needed.
        """
        self._project: Optional[str] = project if project else os.getenv(PROJECT_ENV_NAME)
        if self._project is None:
            raise Exception("Could not find project name in env")

        self._backend: HostedNeptuneBackend = HostedNeptuneBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )
        self._project_qualified_name: Optional[str] = conform_optional(self._project, QualifiedName)
        self._project_api_object: Project = project_name_lookup(
            backend=self._backend, name=self._project_qualified_name
        )
        self._project_id: UniqueId = self._project_api_object.id
        self.project_identifier = normalize_project_name(
            name=self._project, workspace=self._project_api_object.workspace
        )

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
        """Lists IDs and names of the runs in the project.

        Returns a generator of run info dictionaries `{"sys/id": ..., "sys/name": ...}`.
        """
        step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "1000"))

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=NQLQueryAttribute(
                name="sys/trashed", type=NQLAttributeType.BOOLEAN, operator=NQLAttributeOperator.EQUALS, value=False
            ),
            sort_by="sys/id",
            step_size=step_size,
            columns=["sys/id", "sys/name"],
            use_proto=True,
        )

        for row in Table(
            backend=self._backend, container_type=ContainerType.RUN, entries=leaderboard_entries
        ).to_rows():
            yield {
                "sys/id": get_attribute_value_from_entry(entry=row, name="sys/id"),
                "sys/name": get_attribute_value_from_entry(entry=row, name="sys/name"),
            }

    def fetch_read_only_runs(self, with_ids: List[str]) -> Generator[ReadOnlyRun, None, None]:
        """Lists runs of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyRun` instances.

        Args:
            with_ids: List of run ids to fetch.
        """
        for run_id in with_ids:
            yield ReadOnlyRun(read_only_project=self, with_id=run_id)

    def fetch_runs(self) -> "DataFrame":
        """Fetches a table containing IDs and names of runs in the project.

        Returns `pandas.DataFrame` with two columns ('sys/id' and 'sys/name') and rows corresponding to project runs.
        """
        return self.fetch_runs_df(columns=["sys/id", "sys/name"])

    def fetch_runs_df(
        self,
        columns: Optional[Iterable[str]] = None,
        with_ids: Optional[Iterable[str]] = None,
        states: Optional[Iterable[str]] = None,
        owners: Optional[Iterable[str]] = None,
        tags: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Union[bool, Optional[ProgressBarType]] = None,
    ) -> "DataFrame":
        """Fetches the runs' metadata and returns them as a pandas DataFrame.

        Args:
            columns: None or a list of column names to include in the result.
                Defaults to None, which includes all available columns up to 10k.
            with_ids: A list of run IDs to filter the results.
            states: A list of run states to filter the results.
            owners: A list of owner names to filter the results.
            tags: A list of tags to filter the results.
            trashed: Whether to return trashed runs as the result.
                If True: return only trashed runs.
                If False (default): return only non-trashed runs.
                If None: return all runs.
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.

        Returns:
            DataFrame: A pandas DataFrame containing information about the fetched runs.

        Example:
            ```
            # Fetch all runs with specific columns
            columns_to_fetch = ["sys/name", "sys/modification_time", "training/lr"]
            runs_df = my_project.fetch_runs_df(columns=columns_to_fetch, states=["active"])

            # Fetch runs by specific IDs
            specific_run_ids = ["RUN-123", "RUN-456"]
            specific_runs_df = my_project.fetch_runs_df(with_ids=specific_run_ids)
            ```
        """
        query = prepare_nql_query(with_ids, states, owners, tags, trashed)

        if columns is not None:
            # always return entries with `sys/id` column when filter applied
            columns = set(columns)
            columns.add("sys/id")

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=query,
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
            use_proto=True,
        )

        return Table(
            backend=self._backend,
            container_type=ContainerType.RUN,
            entries=leaderboard_entries,
        ).to_pandas()
