#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
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

import re
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
)
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import query_attribute_definitions_within_project
from neptune_retrieval_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)

from neptune_fetcher.alpha import filter
from neptune_fetcher.alpha.internal import (
    env,
    retry,
)

__ALL__ = ("find_attribute_definitions",)

_DEFAULT_BATCH_SIZE = 10_000


def find_attribute_definitions(
    client: AuthenticatedClient,
    project_ids: Iterable[str],
    experiment_ids: Iterable[str],
    attribute_filter: filter.BaseAttributeFilter,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    executor: Optional[Executor] = None,
) -> list[str]:
    if isinstance(attribute_filter, filter.AttributeFilter):
        return _find_attribute_definitions_single(
            client=client,
            project_ids=project_ids,
            experiment_ids=experiment_ids,
            attribute_filter=attribute_filter,
            batch_size=batch_size,
        )
    elif isinstance(attribute_filter, filter._AttributeFilterAlternative):

        def go(child: filter.BaseAttributeFilter) -> list[str]:
            return find_attribute_definitions(
                client=client,
                project_ids=project_ids,
                experiment_ids=experiment_ids,
                attribute_filter=child,
                batch_size=batch_size,
                executor=executor,
            )

        if executor is None:
            with _create_executor() as executor:
                results = executor.map(go, attribute_filter.filters)
        else:
            results = executor.map(go, attribute_filter.filters)
        return list(set().union(*results))
    else:
        raise ValueError(f"Unexpected filter type: {type(attribute_filter)}")


def _create_executor() -> Executor:
    max_workers = env.NEPTUNE_FETCHER_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def _find_attribute_definitions_single(
    client: AuthenticatedClient,
    project_ids: Iterable[str],
    experiment_ids: Iterable[str],
    attribute_filter: filter.AttributeFilter,
    batch_size: int,
) -> list[str]:
    params: dict[str, Any] = {
        "projectIdentifiers": list(project_ids),
        "experimentIdsFilter": list(experiment_ids),
        "attributeNameFilter": dict(),
        "nextPage": {"limit": batch_size},
    }

    must_match_regexes = _union_options(
        [
            _escape_name_eq(_variants_to_list(attribute_filter.name_eq)),
            _variants_to_list(attribute_filter.name_matches_all),
        ]
    )
    if must_match_regexes is not None:
        params["attributeNameFilter"]["mustMatchRegexes"] = must_match_regexes

    must_not_match_regexes = _variants_to_list(attribute_filter.name_matches_none)
    if must_not_match_regexes is not None:
        params["attributeNameFilter"]["mustNotMatchRegexes"] = must_not_match_regexes

    attribute_types = _variants_to_list(attribute_filter.type_in)
    if attribute_types is not None:
        params["attributeFilter"] = [{"attributeType": filter._map_attribute_type(_type)} for _type in attribute_types]

    # note: attribute_filter.aggregations is intentionally ignored

    result: list[str] = []
    next_page_token = None
    while True:
        if next_page_token is not None:
            params["nextPage"]["nextPageToken"] = next_page_token

        body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

        response = retry.backoff_retry(
            lambda: query_attribute_definitions_within_project.sync_detailed(client=client, body=body)
        )

        data: QueryAttributeDefinitionsResultDTO = response.parsed

        result.extend(entry.name for entry in data.entries)

        next_page_token = data.next_page.next_page_token
        if not next_page_token:
            break

    return result


def _escape_name_eq(names: Optional[list[str]]) -> Optional[list[str]]:
    if not names:
        return None

    escaped = [f"{re.escape(name)}" for name in names]

    if len(escaped) == 1:
        return [f"^{escaped[0]}$"]
    else:
        joined = "|".join(escaped)
        return [f"^({joined})$"]


def _variants_to_list(param: Union[str, Iterable[str], None]) -> Optional[list[str]]:
    if param is None:
        return None
    if isinstance(param, str):
        return [param]
    return list(param)


def _map(value: Optional[list[str]], func: Callable[[str], str]) -> Optional[list[str]]:
    if value is None:
        return None
    return [func(value) for value in value]


def _union_options(options: list[Optional[list[str]]]) -> Optional[list[str]]:
    result = None

    for option in options:
        if option is not None:
            if result is None:
                result = []
            result.extend(option)

    return result
