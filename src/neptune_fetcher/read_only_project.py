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
__all__ = ["ReadOnlyProject"]

import collections
import concurrent.futures
import datetime
import logging
import math
import os
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Final,
    Generator,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas
from neptune_api.models import (
    AttributeNameFilterDTO,
    ProjectDTO,
    QueryAttributesBodyDTO,
    SearchLeaderboardEntriesParamsDTO,
)
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import ProtoAttributeDTO
from pandas import DataFrame

from neptune_fetcher.api.api_client import ApiClient
from neptune_fetcher.nql import (
    NQLAggregator,
    NQLAttributeOperator,
    NQLAttributeType,
    NQLQuery,
    NQLQueryAggregate,
    NQLQueryAttribute,
    RawNQLQuery,
    prepare_nql_query,
)
from neptune_fetcher.read_only_run import ReadOnlyRun
from neptune_fetcher.util import (
    NeptuneWarning,
    escape_nql_criterion,
    getenv_int,
    warn_unsupported_value_type,
)

logger = logging.getLogger(__name__)

PROJECT_ENV_NAME = "NEPTUNE_PROJECT"
API_TOKEN_ENV_NAME: Final[str] = "NEPTUNE_API_TOKEN"
SYS_ID = "sys/id"
SYS_COLUMNS = [SYS_ID, "sys/name", "sys/custom_run_id"]

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


@dataclass
class _AttributeContainer:
    id: str
    attributes: Dict[str, Optional[str]]


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
            raise ValueError("Project name not found in the environment. Ensure that NEPTUNE_PROJECT is set.")

        api_token = api_token if api_token else os.getenv(API_TOKEN_ENV_NAME)
        if api_token is None:
            raise ValueError("API token not found in the environment. Ensure that NEPTUNE_API_TOKEN is set.")

        self._backend = ApiClient(api_token=api_token, proxies=proxies)

        _project = self._project_name_lookup(self._project)
        self._project_key: str = _project.project_key
        self._project_id: str = _project.id
        self.project_identifier = f"{_project.organization_name}/{_project.name}"

    def _project_name_lookup(self, name: Optional[str] = None) -> ProjectDTO:
        if not name:
            name = os.getenv(PROJECT_ENV_NAME)
        if not name:
            raise ValueError("Project name is not provided.")
        return self._backend.project_name_lookup(name=name)

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
        for run in list_objects_from_project(self._backend, self._project_id, object_type="run", columns=SYS_COLUMNS):
            yield {column: run.attributes.get(column, None) for column in SYS_COLUMNS}

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
        for exp in list_objects_from_project(
            self._backend, self._project_id, object_type="experiment", columns=SYS_COLUMNS
        ):
            yield {column: exp.attributes.get(column, None) for column in SYS_COLUMNS}

    def fetch_read_only_runs(
        self, with_ids: Optional[list[str]] = None, custom_ids: Optional[list[str]] = None, eager_load_fields=True
    ) -> Iterator[ReadOnlyRun]:
        """Lists runs of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyRun` instances.

        Args:
            with_ids: List of run ids to fetch.
            custom_ids: List of custom run ids to fetch.
            eager_load_fields: Whether to eagerly load the run fields definitions.
                If `False`, individual fields are loaded only when accessed. Default is `True`.
        """

        queries = []
        if with_ids:
            queries.append(_make_leaderboard_nql(is_run=True, with_ids=with_ids))

        if custom_ids:
            queries.append(_make_leaderboard_nql(is_run=True, custom_ids=custom_ids))

        for query in queries:
            runs = list_objects_from_project(
                backend=self._backend,
                project_id=self._project_id,
                query=str(query),
                object_type="run",
                columns=[SYS_ID],
            )

            for run in runs:
                yield ReadOnlyRun._create(
                    read_only_project=self, sys_id=run.attributes[SYS_ID], eager_load_fields=eager_load_fields
                )

    def fetch_read_only_experiments(
        self, names: Optional[list[str]] = None, eager_load_fields=True
    ) -> Iterator[ReadOnlyRun]:
        """Lists experiments of the project in the form of read-only runs.

        Returns a generator of `ReadOnlyRun` instances.

        Args:
            names: List of experiment names to fetch.
            eager_load_fields: Whether to eagerly load the run fields definitions.
                If `False`, individual fields are loaded only when accessed. Default is `True`.

        Example:
            ```
            project = ReadOnlyProject()
            for run in project.fetch_read_only_experiments(names=["yolo-v2", "yolo-v3"]):
                ...
            ```
        """
        if names is None or names == []:
            return
        query = _make_leaderboard_nql(is_run=False, names=names)
        experiments = list_objects_from_project(
            backend=self._backend,
            project_id=self._project_id,
            query=str(query),
            object_type="experiment",
            columns=[SYS_ID],
        )
        for exp in experiments:
            yield ReadOnlyRun._create(
                read_only_project=self, sys_id=exp.attributes[SYS_ID], eager_load_fields=eager_load_fields
            )

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
        progress_bar: bool = True,
        query: Optional[str] = None,
    ) -> "DataFrame":
        """Fetches the runs' metadata and returns them as a pandas DataFrame.

        Args:
            columns: Columns to include in the result, as a list of field names.
                Defaults to None, which includes `sys/custom_run_id` and `sys/name` columns.
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
            progress_bar: Set to `False` to disable the download progress bar.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                The query is applied on top of other criteria like, `custom_ids`, `tags` etc,
                using the logical AND operator.

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

            # Fetch runs with a complex query coupled with other filters
            runs_df = my_project.fetch_runs_df(
                with_ids=specific_run_ids,
                query='(last(`accuracy`:floatSeries) > 0.88) AND (`learning_rate`:float < 0.01)'
            )
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
        progress_bar: bool = True,
        query: Optional[str] = None,
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
            progress_bar: Set to `False` to disable the download progress bar.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                The query is applied on top of other criteria like, `names_regex`, `tags` etc,
                using the logical AND operator.

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

            # Fetch experiments with a complex query coupled with other filters
            experiments_df = my_project.fetch_experiments_df(
                names_regex="tests-.*"
                query='(last(`accuracy`:floatSeries) > 0.88) AND (`learning_rate`:float < 0.01)'
            )
            ```
        """

        _verify_name_regex("names_regex", names_regex)
        _verify_name_regex("names_exclude_regex", names_exclude_regex)

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
        progress_bar: bool = True,
        object_type: Literal["run", "experiment"] = "run",
        query: Optional[str] = None,
    ) -> "DataFrame":
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
            object_type=object_type,
        )

        # Accumulate list dicts containing attributes. Map short_run_id -> dict(attr_path -> value)
        acc = collections.defaultdict(dict)

        # Keep track of all short run ids encountered, in order as they appeared.
        # We use this list to maintain the sorting order as returned by the backend during the
        # initial filtering of runs.
        # This is because the request for field values always sorts the result by (run_id, path).
        all_run_uuids = []

        # Workers fetching attributes in parallel
        futures = []

        value_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for run_uuids in _batch_run_ids(runs_generator, batch_size=FETCH_RUNS_BATCH_SIZE):
                all_run_uuids.extend(run_uuids)

                if len(all_run_uuids) > limit:
                    raise ValueError(
                        f"The number of runs returned exceeds the limit of {limit}. "
                        "Please narrow down your query or provide a smaller 'limit' "
                        "as an argument"
                    )

                # Scatter
                if columns:
                    futures.append(
                        executor.submit(self._fetch_columns_batch, run_uuids, columns=columns, columns_regex=None)
                    )

                if columns_regex:
                    futures.append(
                        executor.submit(self._fetch_columns_batch, run_uuids, columns=None, columns_regex=columns_regex)
                    )

            # Gather
            for future in concurrent.futures.as_completed(futures):
                count, values = future.result()
                value_count += count

                if value_count > WARN_AT_DATAFRAME_SIZE:
                    _warn_large_dataframe(WARN_AT_DATAFRAME_SIZE)

                for run_id, attrs in values.items():
                    acc[run_id].update(attrs)

        df = _to_pandas_df(all_run_uuids, acc, columns)

        return df

    def _fetch_columns_batch(
        self, run_uuids: list[str], columns: Optional[Iterable[str]] = None, columns_regex: Optional[str] = None
    ) -> Tuple[int, Dict[str, Dict[str, Any]]]:
        """
        Called as a worker function concurrently.

        Fetch a batch of columns for the given runs. Return a tuple of the number of
        values fetched, and a dictionary mapping run UUID -> (attr_path -> value).
        """

        acc = collections.defaultdict(dict)
        count = 0

        for run_id, attr_name, value in self._stream_attributes(
            run_uuids, columns=columns, columns_regex=columns_regex, batch_size=FETCH_COLUMNS_BATCH_SIZE
        ):
            acc[run_id][attr_name] = value
            count += 1

        return count, acc

    def _stream_attributes(
        self, run_uuids, *, columns=None, columns_regex=None, limit: Optional[int] = None, batch_size=10_000
    ) -> Generator[Tuple[str, str, Any], None, None]:
        """
        Download attributes that match the given criteria, for the given runs. Attributes are downloaded
        in batches of `batch_size` values per HTTP request.

        The returned generator yields tuples of (run UUID, attribute) for each attribute returned, until
        there is no more data, or the provided `limit` is reached. Limit is calculated as the number of
        non-null data cells returned.
        """

        # `remaining` tracks the number of attributes left to yield, if limit is provided.
        remaining = limit or math.inf
        next_page_token = None

        if columns and columns_regex:
            raise ValueError("Only one of 'columns' and 'columns_regex' can be provided.")

        while True:
            body = QueryAttributesBodyDTO.from_dict(
                {
                    "experimentIdsFilter": run_uuids,
                    "attributeNamesFilter": columns,
                    "nextPage": {"limit": min(remaining, batch_size), "nextPageToken": next_page_token},
                }
            )
            if columns_regex:
                body.attribute_name_filter = AttributeNameFilterDTO.from_dict({"mustMatchRegexes": [columns_regex]})

            data = self._backend.query_attributes_within_project(self._project, body)

            # We're assuming that the backend does not return more entries than requested
            for entry in data.entries:
                for attr in entry.attributes:
                    # Experiment state is not supported in proto
                    if attr.type != "experimentState":
                        yield entry.experimentId, attr.name, _extract_value(attr)

                remaining -= len(entry.attributes)
                if remaining <= 0:
                    break

            next_page_token = data.nextPage.nextPageToken

            # Limit reached, or server doesn't have more data
            if remaining <= 0 or not next_page_token:
                break

    def _fetch_sys_id(
        self, sys_id: Optional[str] = None, custom_id: Optional[str] = None, experiment_name: Optional[str] = None
    ) -> Optional[str]:
        if sys_id is not None:
            query = _make_leaderboard_nql(with_ids=[sys_id], trashed=False)
            object_type = "run"

        elif custom_id is not None:
            query = _make_leaderboard_nql(custom_ids=[custom_id], trashed=False)
            object_type = "run"

        elif experiment_name is not None:
            query = _make_leaderboard_nql(names=[experiment_name], trashed=False)
            object_type = "experiment"

        container = list(
            list_objects_from_project(
                backend=self._backend,
                project_id=self._project_id,
                limit=1,
                columns=[SYS_ID],
                query=str(query),
                object_type=object_type,
            )
        )
        if len(container) == 0:
            return None
        return container[0].attributes[SYS_ID]


def _extract_value(attr: ProtoAttributeDTO) -> Any:
    if attr.type == "string":
        return attr.string_properties.value
    elif attr.type == "int":
        return attr.int_properties.value
    elif attr.type == "float":
        return attr.float_properties.value
    elif attr.type == "bool":
        return attr.bool_properties.value
    elif attr.type == "datetime":
        return datetime.datetime.fromtimestamp(attr.datetime_properties.value / 1000, tz=datetime.timezone.utc)
    elif attr.type == "stringSet":
        return ",".join(attr.string_set_properties.value)
    elif attr.type == "floatSeries":
        return attr.float_series_properties.last
    elif attr.type == "experimentState":
        return "experiment_state"
    else:
        warn_unsupported_value_type(attr.type)
        return None


def _ensure_default_columns(columns: Optional[list[str]], *, sort_by: str) -> list[str]:
    """
    Extend the columns to fetch to include the sorting column and sys/custom_run_id, if it's not already present.
    As a side effect, all columns will be unique.
    """

    if columns is None:
        columns = []

    columns = set(columns)

    for col in (sort_by, "sys/custom_run_id", "sys/name"):
        if col not in columns:
            columns.add(col)

    return list(columns)


def _to_pandas_df(run_uuids: list[str], items: Dict[str, Any], ensure_columns=None) -> DataFrame:
    """
    Convert the provided items into a pandas DataFrame, ensuring the order rows is the same as run_uuids.
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

    df = DataFrame(items[x] for x in run_uuids)

    new_columns = set(df.columns)
    if ensure_columns:
        new_columns = new_columns.union(set(ensure_columns))

    df = df.reindex(sorted(new_columns, key=sort_key), axis="columns", fill_value=pandas.NA)

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

        user_query = RawNQLQuery("(" + query + ")")
    else:
        user_query = None

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
        user_query=user_query,
    )

    if len(str(query)) > MAX_QUERY_LENGTH:
        raise ValueError(
            "Please narrow down the filtering rules. To shorten the query, you can use parameters like `with_ids`, "
            "`custom_ids`, `states`, `tags`, or `owners`."
        )

    return query


def _stream_runs(
    backend: ApiClient,
    project_id: str,
    query: NQLQuery,
    limit: Optional[int],
    sort_by: str,
    ascending: bool,
    object_type: Literal["run", "experiment"] = "run",
) -> Generator[_AttributeContainer, None, None]:
    sort_type: str = _find_sort_type(backend, project_id, sort_by)

    return list_objects_from_project(
        backend=backend,
        project_id=project_id,
        object_type=object_type,
        columns=[],
        query=str(query),
        limit=limit,
        sort_by=(sort_by, sort_type, "ascending" if ascending else "descending"),
    )


def _find_sort_type(backend, project_id, sort_by):
    if sort_by == SYS_ID:
        return "string"
    elif sort_by == "sys/creation_time":
        return "datetime"
    else:
        types = backend.find_field_type_within_project(project_id, sort_by)
        if len(types) == 0:
            warnings.warn(f"Could not find sorting column type for field '{sort_by}'.", NeptuneWarning)
            return "string"
        elif len(types) == 1:
            return types.pop()
        else:
            sorted_types = sorted(types)
            warnings.warn(
                f"Found multiple types for sorting field '{sort_by}': {sorted_types}. Using {sorted_types[0]}.",
                NeptuneWarning,
            )
            return sorted_types[0]


def _batch_run_ids(
    runs: Generator[_AttributeContainer, None, None], *, batch_size: int
) -> Generator[list[str], None, None]:
    """
    Consumes the `runs` generator, yielding lists of Run UUIDs. The length of a single list is limited by `batch_size`.
    """

    batch = []
    for run in runs:
        batch.append(run.id)
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
    names: Optional[list[str]] = None,
    names_exclude_regex: Optional[Union[str, Iterable[str]]] = None,
    custom_id_regex: Optional[Union[str, Iterable[str]]] = None,
    is_run: bool = True,
    user_query: Optional[NQLQuery] = None,
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
                            value=escape_nql_criterion(custom_id),
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
                        value=escape_nql_criterion(regex),
                    ),
                ],
                aggregator=NQLAggregator.AND,
            )

    if names is not None:
        query = NQLQueryAggregate(
            items=[
                query,
                NQLQueryAggregate(
                    items=[
                        NQLQueryAttribute(
                            name="sys/name",
                            type=NQLAttributeType.STRING,
                            operator=NQLAttributeOperator.EQUALS,
                            value=escape_nql_criterion(name),
                        )
                        for name in names
                    ],
                    aggregator=NQLAggregator.OR,
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
                        value=escape_nql_criterion(regex),
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
                    value=escape_nql_criterion(custom_id_regex),
                ),
            ],
            aggregator=NQLAggregator.AND,
        )

    items = [query] if is_run else [query, query_for_experiments_not_runs()]
    if user_query is not None:
        items.append(user_query)

    query = NQLQueryAggregate(
        items=items,
        aggregator=NQLAggregator.AND,
    )

    return query


def query_for_not_trashed() -> NQLQuery:
    return NQLQueryAttribute(
        name="sys/trashed",
        type=NQLAttributeType.BOOLEAN,
        operator=NQLAttributeOperator.EQUALS,
        value=False,
    )


def query_for_experiments_not_runs() -> NQLQuery:
    return NQLQueryAttribute(
        name="sys/experiment/is_head",
        type=NQLAttributeType.BOOLEAN,
        operator=NQLAttributeOperator.EQUALS,  # noqa
        value=True,
    )


def list_objects_from_project(
    backend: ApiClient,
    project_id: str,
    object_type: Literal["run", "experiment"],
    columns: Iterable[str] = None,
    query: str = "(`sys/trashed`:bool = false)",
    limit: Optional[int] = None,
    sort_by: Tuple[str, str, Literal["ascending", "descending"]] = (SYS_ID, "string", "descending"),
) -> Generator[_AttributeContainer, None, None]:
    offset = 0
    batch_size = 10_000
    while True:

        page_limit = min(batch_size, limit - offset) if limit else batch_size
        sort_name, sort_type, sort_direction = sort_by
        body = SearchLeaderboardEntriesParamsDTO.from_dict(
            {
                "attributeFilters": [{"path": name} for name in columns],
                "pagination": {"limit": page_limit, "offset": offset},
                "experimentLeader": object_type == "experiment",
                "query": {"query": query},
                "sorting": {
                    "aggregationMode": "none",
                    "dir": sort_direction,
                    "sortBy": {"name": sort_name, "type": sort_type},
                },
            }
        )
        proto_data = backend.search_entries(project_id, body)

        runs = []
        for run in proto_data.entries:
            attributes = {}
            for attr in run.attributes:
                attributes[attr.name] = attr.string_properties.value
            runs.append(_AttributeContainer(run.experiment_id, attributes))

        yield from runs
        if len(runs) < page_limit:
            break  # No more results to fetch

        offset += len(runs)

        if limit and offset >= limit:
            break


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
    collection: Optional[list[str]],
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
    from neptune_fetcher.util import NeptuneWarning

    warnings.warn(
        f"You have requested a dataset that is over {max_size} entries large. "
        "This might result in long data fetching. Consider narrowing down your query or using the `limit` parameter. "
        "You can use the NEPTUNE_WARN_AT_DATAFRAME_SIZE environment variable to raise the warning threshold, "
        "or set it to 0 to disable this warning.",
        NeptuneWarning,
    )
