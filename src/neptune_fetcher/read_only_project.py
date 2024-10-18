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

import collections
import concurrent.futures
import math
import os
from enum import Enum
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas
from neptune.api.models import (
    Field,
    FieldType,
    LeaderboardEntry,
    NextPage,
)
from neptune.api.searching_entries import find_attribute
from neptune.envs import PROJECT_ENV_NAME
from neptune.exceptions import NeptuneException
from neptune.internal.backends.api_model import Project
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.backends.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
    RawNQLQuery,
)
from neptune.internal.backends.project_name_lookup import project_name_lookup
from neptune.internal.container_type import ContainerType
from neptune.internal.credentials import Credentials
from neptune.internal.id_formats import (
    QualifiedName,
    UniqueId,
    conform_optional,
)
from neptune.internal.utils.logger import get_logger
from neptune.internal.warnings import (
    NeptuneWarning,
    warn_once,
)
from neptune.management.internal.utils import normalize_project_name
from neptune.objects.utils import prepare_nql_query
from neptune.table import Table
from neptune.typing import ProgressBarType
from pandas import DataFrame
from typing_extensions import Literal

from neptune_fetcher.read_only_run import (
    ReadOnlyRun,
    get_attribute_value_from_entry,
)
from neptune_fetcher.util import getenv_int

logger = get_logger()

SYS_COLUMNS = ["sys/id", "sys/name", "sys/custom_run_id"]

MAX_CUMULATIVE_LENGTH = 100000
MAX_QUERY_LENGTH = 250000
MAX_COLUMNS_ALLOWED = 5000
MAX_ELEMENTS_ALLOWED = 5000
MAX_RUNS_ALLOWED = 5000
FETCH_COLUMNS_BATCH_SIZE = getenv_int("NEPTUNE_FETCH_COLUMNS_BATCH_SIZE", 10_000)
FETCH_RUNS_BATCH_SIZE = getenv_int("NEPTUNE_FETCH_RUNS_BATCH_SIZE", 1000)
MAX_WORKERS = getenv_int("NEPTUNE_FETCHER_MAX_WORKERS", 32)

# Issue a warning about a large dataset when no limit is provided by the user,
# and we've reached that many entries. Value of 0 means "Don't warn".
WARN_AT_DATAFRAME_SIZE = getenv_int("NEPTUNE_WARN_AT_DATAFRAME_SIZE", 1_000_000, positive=False)
if WARN_AT_DATAFRAME_SIZE <= 0:
    WARN_AT_DATAFRAME_SIZE = math.inf


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
            raise ValueError("Could not find project name in environment. Make sure NEPTUNE_PROJECT is set.")

        self._backend: HostedNeptuneBackend = HostedNeptuneBackend(
            credentials=Credentials.from_token(api_token=api_token), proxies=proxies
        )

        self._project_api_object: Project = project_name_lookup(
            backend=self._backend, name=conform_optional(self._project, QualifiedName)
        )
        self._project_key: str = self._project_api_object.sys_id
        self._project_id: UniqueId = self._project_api_object.id
        self.project_identifier = normalize_project_name(
            name=self._project, workspace=self._project_api_object.workspace
        )

    def list_runs(self) -> Generator[Dict[str, Optional[str]], None, None]:
        """Lists all runs of a project.

        Returns a generator of dictionaries with run identifiers:
            `{"sys/id": ..., "sys/name": ..., "sys/custom_run_id": ...}`.

        Example:
            ```
            project = ReadOnlyProject("workspace/project", api_token="...")
            for run in project.list_runs():
                print(run)
            ```
        """
        yield from list_objects_from_project(self._backend, self._project_id, object_type="run")

    def list_experiments(self) -> Generator[Dict[str, Optional[str]], None, None]:
        """Lists all experiments of a project.

        Returns a generator of dictionaries with experiment identifiers and names:
        `{"sys/id": ..., "sys/custom_experiment_id": ..., "sys/name": ...}`.

        Example:
            ```
            project = ReadOnlyProject("workspace/project", api_token="...")
            for experiment in project.list_experiments():
                print(experiment)
            ```
        """
        yield from list_objects_from_project(self._backend, self._project_id, object_type="experiment")

    def fetch_read_only_runs(
        self,
        with_ids: Optional[List[str]] = None,
        custom_ids: Optional[List[str]] = None,
    ) -> Iterator[ReadOnlyRun]:
        """Lists runs of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyRun` instances.

        Args:
            with_ids: List of run ids to fetch.
            custom_ids: List of custom run ids to fetch.
        """
        for run_id in with_ids or []:
            yield ReadOnlyRun(read_only_project=self, with_id=run_id)

        for custom_id in custom_ids or []:
            yield ReadOnlyRun(read_only_project=self, custom_id=custom_id)

    def fetch_read_only_experiments(
        self,
        names: Optional[List[str]] = None,
    ) -> Iterator[ReadOnlyRun]:
        """Lists experiments of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyRun` instances.

        Args:
            names: List of experiment names to fetch.

        Example:
            ```
            project = ReadOnlyProject()
            for run in project.fetch_read_only_experiments(names=["yolo-v2", "yolo-v3"]):
                ...
            ```
        """
        for name in names or []:
            yield ReadOnlyRun(read_only_project=self, experiment_name=name)

    def fetch_runs(self) -> "DataFrame":
        """Fetches a table containing identifiers and names of runs in the project.

        Returns `pandas.DataFrame` with three columns (`sys/id`, `sys/custom_run_id` and `sys/name`)
        and one row for each run.

        Example:
            ```
            project = ReadOnlyProject("workspace/project", api_token="...")
            df = project.fetch_runs()
            ```
        """
        return self.fetch_runs_df(columns=SYS_COLUMNS)

    def fetch_experiments(self) -> "DataFrame":
        """Fetches a table containing identifiers and names of experiments in the project.

        Returns `pandas.DataFrame` with three columns (`sys/id`, `sys/custom_run_id` and `sys/name`)
        and one row for each experiment.

        Example:
            ```
            project = ReadOnlyProject("workspace/project", api_token="...")
            df = project.fetch_experiments()
            ```
        """
        return self.fetch_experiments_df(columns=SYS_COLUMNS)

    def fetch_runs_df(
        self,
        columns: Optional[Iterable[str]] = None,
        columns_regex: Optional[str] = None,
        names_regex: Optional[str] = None,
        custom_id_regex: Optional[str] = None,
        with_ids: Optional[Iterable[str]] = None,
        custom_ids: Optional[Iterable[str]] = None,
        states: Optional[Iterable[str]] = None,
        owners: Optional[Iterable[str]] = None,
        tags: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Union[bool, Optional[ProgressBarType]] = None,
        query: Optional[str] = None,
        match_columns_to_filters: bool = True,
    ) -> "DataFrame":
        """Fetches the runs' metadata and returns them as a pandas DataFrame.

        Args:
            columns: Columns to include in the result, as a list of field names.
                Defaults to None, which includes only `sys/custom_run_id` column.
                When using one or both of the `columns` and `columns_regex` parameters,
                the total number of matched columns must not exceed 5000.
            columns_regex: A regex pattern to filter columns by name.
                Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
                When using one or both of the `columns` and `columns_regex` parameters,
                the total number of matched columns must not exceed 5000.
            names_regex: A regex pattern to filter the runs by name.
            custom_id_regex: A regex pattern to filter the runs by custom ID.
            with_ids: A list of run IDs to filter the results.
            custom_ids: A list of custom run IDs to filter the results.
            states: A list of run states to filter the results.
            owners: A list of owner names to filter the results.
            tags: A list of tags to filter the results.
            trashed: Whether to return trashed runs as the result.
                If True: return only trashed runs.
                If False (default): return only non-trashed runs.
                If None: return all runs.
            limit: The maximum number of rows (runs) to return. If `None`, all entries are returned, up to
                   the hard limit of 5000.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters.
            match_columns_to_filters: This argument is deprecated, and always assumed to be `True` as per the
                original description:
                  Whether to subset the columns filtered by `columns_regex`, to only look
                  at the runs that match the filters (e.g. `names_regex`, `custom_id_regex`, `with_ids`, `custom_ids`).
                  If set to `True`, the total number of runs that match the filters must not exceed 5000.
                  The default value of `False` will result in matching the `column_regex` to all columns in the project.

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

            # Fetch runs with a complex query
            runs_df = my_project.fetch_runs_df(query="(accuracy: float > 0.88) AND (loss: float < 0.2)")
            ```
        """
        return self._fetch_df(
            columns=columns,
            columns_regex=columns_regex,
            names_regex=names_regex,
            custom_id_regex=custom_id_regex,
            with_ids=with_ids,
            custom_ids=custom_ids,
            states=states,
            owners=owners,
            tags=tags,
            trashed=trashed,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
            query=query,
        )

    def fetch_experiments_df(
        self,
        columns: Optional[Iterable[str]] = None,
        columns_regex: Optional[str] = None,
        names_regex: Optional[Union[str, Iterable[str]]] = None,
        names_exclude_regex: Optional[Union[str, Iterable[str]]] = None,
        custom_id_regex: Optional[str] = None,
        with_ids: Optional[Iterable[str]] = None,
        custom_ids: Optional[Iterable[str]] = None,
        states: Optional[Iterable[str]] = None,
        owners: Optional[Iterable[str]] = None,
        tags: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: Union[bool, Optional[ProgressBarType]] = None,
        query: Optional[str] = None,
        match_columns_to_filters: bool = True,
    ) -> "DataFrame":
        """Fetches the experiments' metadata and returns them as a pandas DataFrame.

        Args:
            columns: Columns to include in the result, as a list of field names.
                Defaults to None, which includes `sys/custom_run_id` and `sys/name` columns.
                When using one or both of the `columns` and `columns_regex` parameters,
                the total number of matched columns must not exceed 5000.
            columns_regex: A regex pattern to filter columns by name.
                Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
                When using one or both of the `columns` and `columns_regex` parameters,
                the total number of matched columns must not exceed 5000.
            names_regex: A regex pattern or a list of regex patterns to filter the experiments by name.
                Multiple patterns will be connected by AND logic.
            names_exclude_regex: A regex pattern or a list of regex patterns to exclude experiments by name.
                Multiple patterns will be connected by AND logic.
            custom_id_regex: A regex pattern to filter the experiments by custom ID.
            with_ids: A list of experiment IDs to filter the results.
            custom_ids: A list of custom experiment IDs to filter the results.
            states: A list of experiment states to filter the results.
            owners: A list of owner names to filter the results.
            tags: A list of tags to filter the results.
            trashed: Whether to return trashed experiments as the result.
                If True: return only trashed experiments.
                If False (default): return only non-trashed experiments.
                If None: return all experiments.
            limit: The maximum number of rows (experiments) to return. If `None`, all entries are returned, up to
                   the hard limit of 5000.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters.
            match_columns_to_filters: This argument is deprecated, and always assumed to be `True` as per the
                original description:
                  Whether to subset the columns filtered by `columns_regex`, to only look
                  at the runs that match the filters (e.g. `names_regex`, `custom_id_regex`, `with_ids`, `custom_ids`).
                  If set to `True`, the total number of runs that match the filters must not exceed 5000.
                  The default value of `False` will result in matching the `column_regex` to all columns in the project.

        Returns:
            DataFrame: A pandas DataFrame containing information about the fetched experiments.

        Example:
            ```
            # Fetch all experiments with specific columns
            columns_to_fetch = ["sys/name", "sys/modification_time", "training/lr"]
            experiments_df = my_project.fetch_experiments_df(columns=columns_to_fetch, states=["active"])

            # Fetch experiments by specific IDs
            specific_experiment_ids = ["RUN-123", "RUN-456"]
            specific_experiments_df = my_project.fetch_experiments_df(with_ids=specific_experiments_ids)

            # Fetch experiments with a complex query
            experiments_df = my_project.fetch_experiments_df(query="(accuracy: float > 0.88) AND (loss: float < 0.2)")
            ```
        """

        _verify_name_regex("names_regex", names_regex)
        _verify_name_regex("names_exclude_regex", names_exclude_regex)

        if columns is None:
            columns = []

        columns = list(columns) + ["sys/name"]

        return self._fetch_df(
            columns=columns,
            columns_regex=columns_regex,
            names_regex=names_regex,
            names_exclude_regex=names_exclude_regex,
            custom_id_regex=custom_id_regex,
            with_ids=with_ids,
            custom_ids=custom_ids,
            states=states,
            owners=owners,
            tags=tags,
            trashed=trashed,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
            progress_bar=progress_bar,
            object_type="experiment",
            query=query,
        )

    def _fetch_df(
        self,
        columns: Optional[Iterable[str]] = None,
        columns_regex: Optional[str] = None,
        names_regex: Optional[Union[str, Iterable[str]]] = None,
        names_exclude_regex: Optional[Union[str, Iterable[str]]] = None,
        custom_id_regex: Optional[str] = None,
        with_ids: Optional[Iterable[str]] = None,
        custom_ids: Optional[Iterable[str]] = None,
        states: Optional[Iterable[str]] = None,
        owners: Optional[Iterable[str]] = None,
        tags: Optional[Iterable[str]] = None,
        trashed: Optional[bool] = False,
        limit: Optional[int] = None,
        sort_by: str = "sys/creation_time",
        ascending: bool = False,
        progress_bar: ProgressBarType = None,
        object_type: Literal["run", "experiment"] = "run",
        query: Optional[str] = None,
    ) -> "DataFrame":
        if any((with_ids, custom_ids, states, owners, tags)) and query is not None:
            raise ValueError(
                "You can't use the 'query' argument together with the 'with_ids', 'custom_ids', 'states', 'owners', "
                "or 'tags' arguments."
            )

        if limit is not None:
            if limit <= 0:
                raise ValueError("The 'limit' argument must be greater than 0.")
            elif limit > MAX_RUNS_ALLOWED:
                raise ValueError(f"The 'limit' argument can't be higher than {MAX_RUNS_ALLOWED}.")
        else:
            limit = math.inf

        columns = _ensure_default_columns(columns, sort_by=sort_by)

        # Filter out the matching runs based on the provided criteria
        runs_filter = _make_runs_filter_nql(
            query=query,
            trashed=trashed,
            object_type=object_type,
            names_regex=names_regex,
            names_exclude_regex=names_exclude_regex,
            custom_id_regex=custom_id_regex,
            with_ids=with_ids,
            custom_ids=custom_ids,
            states=states,
            owners=owners,
            tags=tags,
        )

        # We will stream over those runs in batches
        runs_generator = _stream_runs(
            backend=self._backend,
            project_id=self._project_id,
            query=runs_filter,
            limit=limit,
            sort_by=sort_by,
            ascending=ascending,
        )

        # Accumulate list dicts containing attributes. Map short_run_id -> dict(attr_path -> value)
        acc = collections.defaultdict(dict)

        # Keep track of all short run ids encountered, in order as they appeared.
        # We use this list to maintain the sorting order as returned by the backend during the
        # initial filtering of runs.
        # This is because the request for field values always sorts the result by (run_id, path).
        all_run_ids = []

        # Workers fetching attributes in parallel
        futures = []

        value_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for run_ids in _batch_run_ids(runs_generator, batch_size=FETCH_RUNS_BATCH_SIZE):
                all_run_ids.extend(run_ids)

                if len(all_run_ids) > limit:
                    raise ValueError(
                        f"The number of runs returned exceeds the limit of {limit}. "
                        "Please narrow down your query or provide a smaller 'limit' "
                        "as an argument"
                    )

                # Scatter
                futures.append(
                    executor.submit(self._fetch_columns_batch, run_ids, columns=columns, columns_regex=columns_regex)
                )

            # Gather
            for future in concurrent.futures.as_completed(futures):
                count, values = future.result()
                value_count += count

                if value_count > WARN_AT_DATAFRAME_SIZE:
                    _warn_large_dataframe(WARN_AT_DATAFRAME_SIZE)

                for run_id, attrs in values.items():
                    acc[run_id].update(attrs)

        df = _to_pandas_df(all_run_ids, acc, columns)

        return df

    def _fetch_columns_batch(
        self, run_ids: List[str], columns: Optional[Iterable[str]] = None, columns_regex: Optional[str] = None
    ) -> Tuple[int, Dict[str, Dict[str, Any]]]:
        """
        Called as a worker function concurrently.

        Fetch a batch of columns for the given runs. Return a tuple of the number of
        values fetched, and a dictionary mapping run_id -> (attr_path -> value).
        """

        acc = collections.defaultdict(dict)
        count = 0

        for run_id, attr in self._stream_attributes(
            run_ids, columns=columns, columns_regex=columns_regex, batch_size=FETCH_COLUMNS_BATCH_SIZE
        ):
            acc[run_id][attr.path] = _extract_value(attr)
            count += 1

        return count, acc

    def _stream_attributes(
        self, run_ids, *, columns=None, columns_regex=None, limit: Optional[int] = None, batch_size=10_000
    ) -> Generator[Tuple[str, Field], None, None]:
        """
        Download attributes that match the given criteria, for the given runs. Attributes are downloaded
        in batches of `batch_size` values per HTTP request.

        The returned generator yields tuples of (run_id, attribute) for each attribute returned, until
        there is no more data, or the provided `limit` is reached. Limit is calculated as the number of
        non-null data cells returned.
        """

        # `remaining` tracks the number of attributes left to yield, if limit is provided.
        remaining = limit or math.inf
        next_page_token = None

        while True:
            # Don't request more than the number or remaining items
            next_page = NextPage(limit=min(remaining, batch_size), next_page_token=next_page_token)

            response = self._backend.query_fields_within_project(
                project_id=self._project,
                experiment_ids_filter=run_ids,
                field_names_filter=columns,
                field_name_regex=columns_regex,
                next_page=next_page,
                use_proto=True,
            )

            # We're assuming that the backend does not return more entries than requested
            for entry in response.entries:
                for attr in entry.fields:
                    yield entry.object_key, attr

                remaining -= len(entry.fields)
                if remaining <= 0:
                    break

            next_page_token = response.next_page.next_page_token

            # Limit reached, or server doesn't have more data
            if remaining <= 0 or not next_page_token:
                break


def _ensure_default_columns(columns: Optional[List[str]], *, sort_by: str) -> List[str]:
    """
    Extend the columns to fetch to include the sorting column and sys/custom_run_id, if it's not already present.
    As a side effect, all columns will be unique.
    """

    if columns is None:
        columns = []

    columns = set(columns)

    for col in (sort_by, "sys/custom_run_id"):
        if col not in columns:
            columns.add(col)

    return list(columns)


def _extract_value(attr: Field):
    if attr.type == FieldType.FLOAT_SERIES:
        return attr.last
    elif attr.type == FieldType.STRING_SET:
        return ",".join(attr.values)

    return attr.value


def _to_pandas_df(order: List, items: Dict[str, Any], ensure_columns=None) -> DataFrame:
    """
    Convert the provided items into a pandas DataFrame, ensuring the order of columns as specified.
    Any columns passed in `ensure_columns` will be present in the result as NA, even if not returned by the backend.

    System and monitoring columns will be sorted to the front.
    """

    def sort_key(field: str) -> Tuple[int, str]:
        namespace = field.split("/")[0]
        if namespace == "sys":
            return 0, field
        if namespace == "monitoring":
            return 2, field
        return 1, field

    df = DataFrame(items[x] for x in order)

    if ensure_columns:
        for col in ensure_columns:
            if col not in df.columns:
                df[col] = pandas.NA

    df = df.reindex(sorted(df.columns, key=sort_key), axis="columns")

    return df


def _make_runs_filter_nql(
    query: Optional[str],
    trashed: bool,
    object_type: Literal["run", "experiment"],
    names_regex: Optional[Union[str, Iterable[str]]],
    names_exclude_regex: Optional[Union[str, Iterable[str]]],
    custom_id_regex: Optional[str],
    with_ids: Optional[Iterable[str]],
    custom_ids: Optional[Iterable[str]],
    states: Optional[Iterable[str]],
    owners: Optional[Iterable[str]],
    tags: Optional[Iterable[str]],
) -> NQLQuery:
    """
    Transforms the user-provided filter arguments into an NQL query for filtering Runs.
    """

    if query is not None:
        if len(query) > MAX_QUERY_LENGTH:
            raise ValueError(
                f"The `query` parameter is too long ({len(query)} characters). "
                f"Please limit the query to {MAX_QUERY_LENGTH} characters or fewer."
            )

        return enrich_user_query(query=query, trashed=trashed, is_run=object_type == "run")

    _verify_string_collection(with_ids, "with_ids", MAX_ELEMENTS_ALLOWED, MAX_CUMULATIVE_LENGTH)
    _verify_string_collection(custom_ids, "custom_ids", MAX_ELEMENTS_ALLOWED, MAX_CUMULATIVE_LENGTH)
    _verify_string_collection(owners, "owners", MAX_ELEMENTS_ALLOWED, MAX_CUMULATIVE_LENGTH)
    _verify_string_collection(tags, "tags", MAX_ELEMENTS_ALLOWED, MAX_CUMULATIVE_LENGTH)

    query = _make_leaderboard_nql(
        with_ids=with_ids,
        custom_ids=custom_ids,
        states=states,
        owners=owners,
        tags=tags,
        trashed=trashed,
        custom_id_regex=custom_id_regex,
        names_regex=names_regex,
        names_exclude_regex=names_exclude_regex,
        is_run=object_type == "run",
    )

    if len(str(query)) > MAX_QUERY_LENGTH:
        raise ValueError(
            "Please narrow down the filtering rules. To shorten the query, you can use parameters like `with_ids`, "
            "`custom_ids`, `states`, `tags`, or `owners`."
        )

    return query


def _stream_runs(
    backend: HostedNeptuneBackend,
    project_id: UniqueId,
    query: NQLQuery,
    limit: Optional[int],
    sort_by: str,
    ascending: bool,
) -> Generator[LeaderboardEntry, None, None]:
    return backend.search_leaderboard_entries(
        project_id=project_id,
        types=[ContainerType.RUN],
        query=query,
        columns=["sys/id"],
        limit=limit,
        sort_by=sort_by,
        step_size=FETCH_RUNS_BATCH_SIZE,
        ascending=ascending,
        progress_bar=False,
        use_proto=True,
    )


def _batch_run_ids(runs: Generator, *, batch_size: int) -> Generator[List[str], None, None]:
    """
    Consumes the `runs` generator, yields lists of short ids, of the given max size.
    """

    batch = []
    for run in runs:
        run_id = find_attribute(entry=run, path="sys/id")
        if run_id is None:
            raise NeptuneException("Experiment id missing in server response")

        batch.append(run_id.value)
        if len(batch) == batch_size:
            yield batch
            batch = []

    # We could have a leftover partial batch, smaller than requested max size
    if batch:
        yield batch


def _make_leaderboard_nql(
    with_ids: Optional[Iterable[str]] = None,
    custom_ids: Optional[Iterable[str]] = None,
    states: Optional[Iterable[str]] = None,
    owners: Optional[Iterable[str]] = None,
    tags: Optional[Iterable[str]] = None,
    trashed: Optional[bool] = False,
    names_regex: Optional[str] = None,
    names_exclude_regex: Optional[Union[str, Iterable[str]]] = None,
    custom_id_regex: Optional[Union[str, Iterable[str]]] = None,
    is_run: bool = True,
) -> NQLQuery:
    query = prepare_nql_query(ids=with_ids, states=states, owners=owners, tags=tags, trashed=trashed)

    if custom_ids is not None:
        query = NQLQueryAggregate(
            items=[
                query,
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/custom_run_id",
                            type=NQLAttributeType.STRING,
                            operator=NQLAttributeOperator.EQUALS,
                            value=custom_id,
                        )
                        for custom_id in custom_ids
                    ],
                    aggregator=NQLAggregator.OR,
                ),
            ],
            aggregator=NQLAggregator.AND,
        )

    if isinstance(names_regex, str):
        names_regex = [names_regex]

    if names_regex is not None:
        for regex in names_regex:
            query = NQLQueryAggregate(
                items=[
                    query,
                    NQLQueryAttribute(
                        name="sys/name",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.MATCHES,
                        value=regex,
                    ),
                ],
                aggregator=NQLAggregator.AND,
            )

    if isinstance(names_exclude_regex, str):
        names_exclude_regex = [names_exclude_regex]

    if names_exclude_regex is not None:
        for regex in names_exclude_regex:
            query = NQLQueryAggregate(
                items=[
                    query,
                    NQLQueryAttribute(
                        name="sys/name",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.NOT_MATCHES,
                        value=regex,
                    ),
                ],
                aggregator=NQLAggregator.AND,
            )

    if custom_id_regex is not None:
        query = NQLQueryAggregate(
            items=[
                query,
                NQLQueryAttribute(
                    name="sys/custom_run_id",
                    type=NQLAttributeType.STRING,
                    operator=NQLAttributeOperator.MATCHES,
                    value=custom_id_regex,
                ),
            ],
            aggregator=NQLAggregator.AND,
        )

    items = [query] if is_run else [query, query_for_experiments_not_runs()]

    query = NQLQueryAggregate(
        items=items,
        aggregator=NQLAggregator.AND,
    )

    return query


def enrich_user_query(query: str, trashed: Optional[bool], is_run: bool = True) -> NQLQuery:
    """
    Enriches the user-provided NQL query string with additional conditions based on
    the `trashed` flag and whether we're looking for runs or experiments.
    """

    items: List[Union[str, NQLQuery]] = [
        RawNQLQuery("(" + query + ")"),
    ]

    if not is_run:
        items.append(query_for_experiments_not_runs())

    if trashed is not None:
        items.append(
            NQLQueryAttribute(
                name="sys/trashed", type=NQLAttributeType.BOOLEAN, operator=NQLAttributeOperator.EQUALS, value=trashed
            ),
        )

    return NQLQueryAggregate(
        items=items,
        aggregator=NQLAggregator.AND,
    )


def query_for_not_trashed() -> NQLQuery:
    return NQLQueryAttribute(
        name="sys/trashed",
        type=NQLAttributeType.BOOLEAN,
        operator=NQLAttributeOperator.EQUALS,
        value=False,
    )


def query_for_experiments_not_runs() -> NQLQuery:
    names = [(m.name, m.value) for m in NQLAttributeOperator]  # noqa

    # handle the case when the client nql doesn't have the 'NOT_EQUALS' operator
    if "NOT_EQUALS" not in NQLAttributeOperator.__members__:
        names += [("NOT_EQUALS", "!=")]

    operators = Enum("NQLAttributeOperator", names)

    return NQLQueryAttribute(
        name="sys/name",
        type=NQLAttributeType.STRING,
        operator=operators.NOT_EQUALS,  # noqa
        value="",
    )


def list_objects_from_project(
    backend: HostedNeptuneBackend,
    project_id: UniqueId,
    object_type: Literal["run", "experiment"],
) -> List[Dict[str, Optional[str]]]:
    queries = [query_for_not_trashed()]
    if object_type == "experiment":
        queries.append(query_for_experiments_not_runs())

    query = NQLQueryAggregate(
        items=queries,
        aggregator=NQLAggregator.AND,
    )

    leaderboard_entries = backend.search_leaderboard_entries(
        project_id=project_id,
        types=[ContainerType.RUN],
        query=query,
        sort_by="sys/id",
        step_size=FETCH_RUNS_BATCH_SIZE,
        columns=SYS_COLUMNS,
        use_proto=True,
    )
    return [
        {
            "sys/id": get_attribute_value_from_entry(entry=row, name="sys/id"),
            "sys/name": get_attribute_value_from_entry(entry=row, name="sys/name"),
            "sys/custom_run_id": get_attribute_value_from_entry(entry=row, name="sys/custom_run_id"),
        }
        for row in Table(backend=backend, container_type=ContainerType.RUN, entries=leaderboard_entries).to_rows()
    ]


def _verify_name_regex(collection_name: str, name_or_list: Optional[Union[str, Iterable[str]]]) -> None:
    if name_or_list is None:
        return

    if isinstance(name_or_list, str):
        if not name_or_list:
            raise ValueError(f"The {collection_name} regex provided is an empty string, which is not allowed")
    else:
        if not name_or_list:
            raise ValueError(f"The {collection_name} regex list provided cannot be empty")

        if not all(name_or_list):
            raise ValueError(f"The {collection_name} regex list provided contains empty strings, which are not allowed")


def _verify_string_collection(
    collection: Optional[List[str]],
    collection_name: str,
    max_elements_allowed: int,
    max_cumulative_length: int,
) -> None:
    if collection is None:
        return

    if len(collection) > max_elements_allowed:
        raise ValueError(
            f"Too many {collection_name} provided ({len(collection)}). "
            f"Please limit the number of {collection_name} to {max_elements_allowed} or fewer."
        )

    if sum(map(len, collection)) > max_cumulative_length:
        raise ValueError(
            f"Too many characters in the {collection_name} provided ({sum(map(len, collection))}). "
            f"Please limit the total number of characters in {collection_name} to {max_cumulative_length} or fewer."
        )


def _warn_large_dataframe(max_size):
    warn_once(
        f"You have requested a dataset that is over {max_size} entries large. "
        "This might result in long data fetching. Consider narrowing down your query or using the `limit` parameter. "
        "You can use the NEPTUNE_WARN_AT_DATAFRAME_SIZE environment variable to raise the warning threshold, "
        "or set it to 0 to disable this warning.",
        exception=NeptuneWarning,
    )
