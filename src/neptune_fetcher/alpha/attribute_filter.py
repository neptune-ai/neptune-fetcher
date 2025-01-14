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

# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
from abc import ABC
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from typing import (
    Callable,
    Literal,
    Optional,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import query_attribute_definitions_within_project
from neptune_retrieval_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)

from .env import NEPTUNE_FETCHER_MAX_WORKERS
from .retry import backoff_retry

__ALL__ = (
    "AttributeFilter",
    "find_attributes",
)

_DEFAULT_BATCH_SIZE = 1000


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "AttributeFilterAlternative":
        return self.any(self, other)

    def any(*filters: "BaseAttributeFilter") -> "AttributeFilterAlternative":
        return AttributeFilterAlternative(*filters)


@dataclass
class AttributeFilter(BaseAttributeFilter):
    name_eq: Union[str, list[str], None] = None
    type_in: Optional[list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]]] = None
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: Optional[list[Literal["last", "min", "max", "average", "variance", "auto"]]] = None

    def __post_init__(self):
        if self.type_in is None:
            self.type_in = ["float", "int", "string", "bool", "datetime", "float_series", "string_set"]


@dataclass
class AttributeFilterAlternative(BaseAttributeFilter):
    filters: list[AttributeFilter]


def find_attributes(
    client: AuthenticatedClient,
    project_id: str,
    experiment_ids: list[str],
    attribute_filter: BaseAttributeFilter,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    executor: Optional[Executor] = None,
) -> list[str]:
    if isinstance(attribute_filter, AttributeFilter):
        return _find_attributes(
            client=client,
            project_id=project_id,
            experiment_ids=experiment_ids,
            attribute_filter=attribute_filter,
            batch_size=batch_size,
        )
    elif isinstance(attribute_filter, AttributeFilterAlternative):

        def go(child: BaseAttributeFilter) -> list[str]:
            return find_attributes(
                client=client,
                project_id=project_id,
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


def _create_executor() -> Executor:
    max_workers = NEPTUNE_FETCHER_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def _find_attributes(
    client: AuthenticatedClient,
    project_id: str,
    experiment_ids: list[str],
    attribute_filter: AttributeFilter,
    batch_size: int,
) -> list[str]:
    params = {
        "experimentIdsFilter": experiment_ids,
        "attributeNameFilter": {},
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
        params["attributeFilter"] = [{"attributeType": _map_attribute_type(_type)} for _type in attribute_types]

    # note: attribute_filter.aggregations is intentionally ignored

    result = []
    next_page_token = None
    while True:
        if next_page_token is not None:
            params["nextPage"]["nextPageToken"] = next_page_token

        body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

        response = backoff_retry(
            lambda: query_attribute_definitions_within_project.sync_detailed(
                client=client, body=body, project_identifier=project_id
            )
        )

        data: QueryAttributeDefinitionsResultDTO = response.parsed

        result.extend(entry.name for entry in data.entries)

        next_page_token = data.next_page.next_page_token
        if not next_page_token:
            break

    return result


_ATTRIBUTE_TYPE_MAP = {
    "float_series": "floatSeries",
    "string_set": "stringSet",
}


def _map_attribute_type(_type: str) -> str:
    if _type in _ATTRIBUTE_TYPE_MAP:
        return _ATTRIBUTE_TYPE_MAP[_type]
    return _type


def _escape_name_eq(names: Optional[list[str]]) -> Optional[list[str]]:
    if not names:
        return None

    escaped = [f"{re.escape(name)}" for name in names]

    if len(escaped) == 1:
        return [f"^{escaped[0]}$"]
    else:
        joined = "|".join(escaped)
        return [f"^({joined})$"]


def _variants_to_list(param: Union[str, list[str], None]) -> Optional[list[str]]:
    if param is None:
        return None
    if isinstance(param, str):
        return [param]
    return param


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
