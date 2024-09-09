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
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Union,
)

from neptune.api.models import StringField
from neptune.api.pagination import paginate_over
from neptune.envs import (
    NEPTUNE_FETCH_TABLE_STEP_SIZE,
    PROJECT_ENV_NAME,
)
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
from neptune.management.internal.utils import normalize_project_name
from neptune.objects.utils import prepare_nql_query
from neptune.table import Table
from neptune.typing import ProgressBarType
from typing_extensions import Literal

from neptune_fetcher.read_only_run import (
    ReadOnlyRun,
    get_attribute_value_from_entry,
)

if TYPE_CHECKING:
    from pandas import DataFrame


MAX_COLUMNS_ALLOWED = 5000
MAX_RUNS_ALLOWED = 5000
NEPTUNE_FETCH_COLUMNS_STEP_SIZE = "NEPTUNE_FETCH_COLUMNS_STEP_SIZE"


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
        return self.fetch_runs_df(columns=["sys/id", "sys/name", "sys/custom_run_id"])

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
        return self.fetch_experiments_df(columns=["sys/id", "sys/custom_run_id", "sys/name"])

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
        match_columns_to_filters: bool = False,
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
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters.
            match_columns_to_filters: Whether to subset the columns filtered by `columns_regex`, to only look
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
        return self._fetch_project_objects_df(
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
            match_columns_to_filters=match_columns_to_filters,
        )

    def fetch_experiments_df(
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
        match_columns_to_filters: bool = False,
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
            names_regex: A regex pattern to filter the experiments by name.
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
            limit: How many entries to return at most. If `None`, all entries are returned.
            sort_by: Name of the field to sort the results by.
                The field must represent a simple type (string, float, datetime, integer, or Boolean).
            ascending: Whether to sort the entries in ascending order of the sorting column values.
            progress_bar: Set to `False` to disable the download progress bar,
                or pass a `ProgressBarCallback` class to use your own progress bar callback.
            query: A query string to filter the results. Use the Neptune Query Language syntax.
                Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters.
            match_columns_to_filters: Whether to subset the columns filtered by `columns_regex`, to only look
                at the runs that match the filters (e.g. `names_regex`, `custom_id_regex`, `with_ids`, `custom_ids`).
                If set to `True`, the total number of experiments that match the filters must not exceed 5000.

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
        return self._fetch_project_objects_df(
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
            object_type="experiment",
            query=query,
            match_columns_to_filters=match_columns_to_filters,
        )

    def _fetch_project_objects_df(
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
        progress_bar: ProgressBarType = None,
        object_type: Literal["run", "experiment"] = "run",
        query: Optional[str] = None,
        match_columns_to_filters: bool = False,
    ) -> "DataFrame":
        step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "200"))

        if any((with_ids, custom_ids, states, owners, tags)) and query is not None:
            raise ValueError(
                "You can't use the 'query' parameter together with the 'with_ids', 'custom_ids', 'states', 'owners', "
                "or 'tags' parameters."
            )

        prepared_query = _resolve_query(
            query=query,
            trashed=trashed,
            object_type=object_type,
            names_regex=names_regex,
            custom_id_regex=custom_id_regex,
            with_ids=with_ids,
            custom_ids=custom_ids,
            states=states,
            owners=owners,
            tags=tags,
        )

        if _should_subset_columns(
            match_columns_to_filters=match_columns_to_filters,
            columns_regex=columns_regex,
            names_regex=names_regex,
            custom_id_regex=custom_id_regex,
            with_ids=with_ids,
            custom_ids=custom_ids,
        ):
            # make sure filtering columns will be done only on requested runs - not the entire project
            with_ids = _get_ids_matching_filtering_conditions(
                backend=self._backend,
                project_id=self._project_id,
                prepared_query=prepared_query,
                limit=limit,
                sort_by=sort_by,
                ascending=ascending,
                with_ids=with_ids,
                custom_ids=custom_ids,
            )

        columns = _resolve_columns(
            backend=self._backend,
            project_qualified_name=self._project_qualified_name,
            columns=columns,
            columns_regex=columns_regex,
            sort_by=sort_by,
            with_ids=with_ids,
            object_type=object_type,
        )

        leaderboard_entries = self._backend.search_leaderboard_entries(
            project_id=self._project_id,
            types=[ContainerType.RUN],
            query=prepared_query,
            columns=columns,
            limit=limit,
            sort_by=sort_by,
            step_size=step_size,
            ascending=ascending,
            progress_bar=progress_bar,
            use_proto=True,
        )

        return Table(
            backend=self._backend,
            container_type=ContainerType.RUN,
            entries=leaderboard_entries,
        ).to_pandas()


def _should_subset_columns(
    match_columns_to_filters: bool,
    columns_regex: Optional[str],
    names_regex: Optional[str],
    custom_id_regex: Optional[str],
    with_ids: Optional[Iterable[str]],
    custom_ids: Optional[Iterable[str]],
) -> bool:
    if not match_columns_to_filters:
        return False

    return columns_regex and any([names_regex, custom_id_regex, custom_ids, with_ids])


def _resolve_query(
    query: Optional[str],
    trashed: bool,
    object_type: Literal["run", "experiment"],
    names_regex: Optional[str],
    custom_id_regex: Optional[str],
    with_ids: Optional[Iterable[str]],
    custom_ids: Optional[Iterable[str]],
    states: Optional[Iterable[str]],
    owners: Optional[Iterable[str]],
    tags: Optional[Iterable[str]],
) -> NQLQuery:
    if query is not None:
        return build_extended_nql_query(
            query=query,
            trashed=trashed,
            is_run=object_type == "run",
        )

    return prepare_extended_nql_query(
        with_ids=with_ids,
        custom_ids=custom_ids,
        states=states,
        owners=owners,
        tags=tags,
        trashed=trashed,
        custom_id_regex=custom_id_regex,
        names_regex=names_regex,
        is_run=object_type == "run",
    )


def _get_ids_matching_filtering_conditions(
    backend: HostedNeptuneBackend,
    project_id: UniqueId,
    prepared_query: NQLQuery,
    limit: Optional[int],
    sort_by: str,
    ascending: bool,
    with_ids: Optional[Iterable[str]],
    custom_ids: Optional[Iterable[str]],
) -> Optional[Iterable[str]]:
    if with_ids or custom_ids:
        return with_ids

    all_matching_objects = backend.search_leaderboard_entries(
        project_id=project_id,
        types=[ContainerType.RUN],
        query=prepared_query,
        columns=["sys/id"],
        limit=limit,
        sort_by=sort_by,
        step_size=1000,
        ascending=ascending,
        progress_bar=False,
        use_proto=True,
    )
    all_ids_matching_query = set()

    for entry in all_matching_objects:
        id_field = entry.fields[0]
        if not isinstance(id_field, StringField):
            continue
        all_ids_matching_query.add(id_field.value)

        if len(all_ids_matching_query) == MAX_RUNS_ALLOWED:
            raise ValueError("Too many runs matching the filtering conditions. Please narrow down the query.")

    return all_ids_matching_query


def _resolve_columns(
    backend: "HostedNeptuneBackend",
    project_qualified_name: Optional[str],
    columns: Optional[Iterable[str]],
    columns_regex: Optional[str],
    with_ids: Optional[Iterable[str]],
    object_type: Literal["run", "experiment"],
    sort_by: str,
) -> Iterable[str]:
    # always return entries with `sys/custom_run_id` column when filter applied
    required_columns = {"sys/custom_run_id", sort_by}
    if object_type == "experiment":
        required_columns.add("sys/name")

    columns = set(columns) | required_columns if columns else required_columns

    if len(columns) > MAX_COLUMNS_ALLOWED:
        raise ValueError(
            f"Too many columns requested ({len(columns)}). "
            f"Please limit the number of columns to {MAX_COLUMNS_ALLOWED} or fewer."
        )

    if columns_regex is not None and len(columns) < MAX_COLUMNS_ALLOWED:
        columns = filter_columns_regex(
            columns_regex=columns_regex,
            columns=columns,
            backend=backend,
            project_qualified_name=project_qualified_name,
            with_ids=with_ids,
        )

    return columns


def prepare_extended_nql_query(
    with_ids: Optional[Iterable[str]] = None,
    custom_ids: Optional[Iterable[str]] = None,
    states: Optional[Iterable[str]] = None,
    owners: Optional[Iterable[str]] = None,
    tags: Optional[Iterable[str]] = None,
    trashed: Optional[bool] = False,
    names_regex: Optional[str] = None,
    custom_id_regex: Optional[str] = None,
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

    if names_regex is not None:
        query = NQLQueryAggregate(
            items=[
                query,
                NQLQueryAttribute(
                    name="sys/name",
                    type=NQLAttributeType.STRING,
                    operator=NQLAttributeOperator.MATCHES,
                    value=names_regex,
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


def build_extended_nql_query(query: str, trashed: Optional[bool], is_run: bool = True) -> NQLQuery:
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


def filter_columns_regex(
    columns_regex: str,
    columns: Set[str],
    backend: HostedNeptuneBackend,
    project_qualified_name: str,
    with_ids: Optional[List[str]] = None,
) -> Set[str]:
    if MAX_COLUMNS_ALLOWED <= len(columns):
        return columns

    page_size = int(os.getenv(NEPTUNE_FETCH_COLUMNS_STEP_SIZE, "1000"))
    field_definitions = paginate_over(
        getter=backend.query_fields_definitions_within_project,
        extract_entries=lambda data: data.entries,
        project_id=project_qualified_name,
        field_name_regex=columns_regex,
        experiment_ids_filter=with_ids,
        page_size=page_size,
        limit=MAX_COLUMNS_ALLOWED - len(columns),
    )

    for field_definition in field_definitions:
        columns.add(field_definition.path)

    return columns


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
    step_size = int(os.getenv(NEPTUNE_FETCH_TABLE_STEP_SIZE, "1000"))
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
        step_size=step_size,
        columns=["sys/id", "sys/name", "sys/custom_run_id"],
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
