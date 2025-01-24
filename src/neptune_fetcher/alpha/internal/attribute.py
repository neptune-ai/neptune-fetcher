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

import itertools as it
import re
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from typing import (
    Any,
    Generator,
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
    identifiers,
    types,
    util,
)

__ALL__ = ("find_attribute_definitions",)

_DEFAULT_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class AttributeDefinition:
    name: str
    type: str


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filter.BaseAttributeFilter,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    executor: Optional[Executor] = None,
) -> list[AttributeDefinition]:
    if isinstance(attribute_filter, filter.AttributeFilter):
        return [
            item
            for page in _fetch_attribute_definitions_single(
                client=client,
                project_identifiers=project_identifiers,
                experiment_identifiers=experiment_identifiers,
                attribute_filter=attribute_filter,
                batch_size=batch_size,
            )
            for item in page.items
        ]
    elif isinstance(attribute_filter, filter._AttributeFilterAlternative):

        def go(child: filter.BaseAttributeFilter, _executor: Executor) -> list[AttributeDefinition]:
            return fetch_attribute_definitions(
                client=client,
                project_identifiers=project_identifiers,
                experiment_identifiers=experiment_identifiers,
                attribute_filter=child,
                batch_size=batch_size,
                executor=_executor,
            )

        if executor is None:
            with _create_executor() as executor:
                results = executor.map(go, attribute_filter.filters, it.repeat(executor))
                names = list(set().union(*results))
        else:
            results = executor.map(go, attribute_filter.filters, it.repeat(executor))
            names = list(set().union(*results))
        return names
    else:
        raise ValueError(f"Unexpected filter type: {type(attribute_filter)}")


def _create_executor() -> Executor:
    max_workers = env.NEPTUNE_FETCHER_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def _fetch_attribute_definitions_single(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filter.AttributeFilter,
    batch_size: int,
) -> Generator[util.Page[AttributeDefinition], None, None]:
    params: dict[str, Any] = {
        "projectIdentifiers": list(project_identifiers),
        "experimentIdsFilter": list(str(e) for e in experiment_identifiers),
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
        params["attributeFilter"] = [
            {"attributeType": types.map_attribute_type_user_to_backend(_type)} for _type in attribute_types
        ]

    # note: attribute_filter.aggregations is intentionally ignored

    next_page_token = None
    while True:
        if next_page_token is not None:
            params["nextPage"]["nextPageToken"] = next_page_token

        body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

        response = util.backoff_retry(
            query_attribute_definitions_within_project.sync_detailed, client=client, body=body
        )

        data: QueryAttributeDefinitionsResultDTO = response.parsed

        items = []
        for entry in data.entries:
            item = AttributeDefinition(name=entry.name, type=types.map_attribute_type_backend_to_user(str(entry.type)))
            items.append(item)
        yield util.Page(items=items)

        next_page_token = data.next_page.next_page_token
        if not next_page_token:
            break


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


def _union_options(options: list[Optional[list[str]]]) -> Optional[list[str]]:
    result = None

    for option in options:
        if option is not None:
            if result is None:
                result = []
            result.extend(option)

    return result
