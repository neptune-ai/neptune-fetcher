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
import functools as ft
import itertools as it
import re
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import (
    Any,
    Generator,
    Iterable,
    List,
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

import neptune_fetcher.alpha.filters as filters
from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
)
from neptune_fetcher.alpha.internal.api_client import attribute_types as types
from neptune_fetcher.alpha.internal.api_client import util


@dataclass(frozen=True)
class AttributeDefinition:
    name: str
    type: str


@dataclass(frozen=True)
class AttributeDefinitionAggregation:
    attribute_definition: AttributeDefinition
    aggregation: Optional[Literal["last", "min", "max", "average", "variance"]]


def _split_to_tasks(
    _attribute_filter: filters.BaseAttributeFilter,
) -> List[filters.AttributeFilter]:
    if isinstance(_attribute_filter, filters.AttributeFilter):
        return [_attribute_filter]
    elif isinstance(_attribute_filter, filters._AttributeFilterAlternative):
        return list(it.chain.from_iterable(_split_to_tasks(child) for child in _attribute_filter.filters))
    else:
        raise RuntimeError(f"Unexpected filter type: {type(_attribute_filter)}")


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Optional[Iterable[identifiers.ExperimentIdentifier]],
    attribute_filter: filters.BaseAttributeFilter,
    executor: Executor,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[AttributeDefinition], None, None]:
    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, experiment_identifiers, attribute_filter, batch_size, executor
    )

    seen_items: set[AttributeDefinition] = set()
    for page, _filter in pages_filters:
        new_items = [item for item in page.items if item not in seen_items]
        seen_items.update(new_items)
        yield util.Page(items=new_items)


def fetch_attribute_definition_aggregations(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filters.BaseAttributeFilter,
    executor: Executor,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[AttributeDefinitionAggregation], None, None]:
    """
    Each attribute definition is yielded once with aggregation=None when it's first encountered.
    If the attribute definition is of a type that supports aggregations (for now only float_series),
    it's then yielded again for each aggregation in the filter.
    """

    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, experiment_identifiers, attribute_filter, batch_size, executor
    )

    seen_items: set[AttributeDefinitionAggregation] = set()

    for page, _filter in pages_filters:
        new_items = []
        for item in page.items:
            attribute_aggregation = AttributeDefinitionAggregation(attribute_definition=item, aggregation=None)
            if attribute_aggregation not in seen_items:
                new_items.append(attribute_aggregation)
                seen_items.add(attribute_aggregation)

            if item.type == "float_series":
                for aggregation in _filter.aggregations:
                    attribute_aggregation = AttributeDefinitionAggregation(
                        attribute_definition=item, aggregation=aggregation
                    )
                    if attribute_aggregation not in seen_items:
                        new_items.append(attribute_aggregation)
                        seen_items.add(attribute_aggregation)

        yield util.Page(items=new_items)


def _fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Optional[Iterable[identifiers.ExperimentIdentifier]],
    attribute_filter: filters.BaseAttributeFilter,
    batch_size: int,
    executor: Executor,
) -> Generator[tuple[util.Page[AttributeDefinition], filters.AttributeFilter], None, None]:
    def go_fetch_single(_filter: filters.AttributeFilter) -> Generator[util.Page[AttributeDefinition], None, None]:
        return _fetch_attribute_definitions_single_filter(
            client=client,
            project_identifiers=project_identifiers,
            experiment_identifiers=experiment_identifiers,
            attribute_filter=_filter,
            batch_size=batch_size,
        )

    filters_ = _split_to_tasks(attribute_filter)

    if len(filters_) == 1:
        head = filters_[0]
        for page in go_fetch_single(head):
            yield page, head
    else:
        output = util.generate_concurrently(
            items=(_filter for _filter in filters_),
            executor=executor,
            downstream=lambda _filter: util.generate_concurrently(
                items=go_fetch_single(_filter),
                executor=executor,
                downstream=lambda _page: util.return_value((_page, _filter)),
            ),
        )
        yield from util.gather_results(output)


def _fetch_attribute_definitions_single_filter(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Optional[Iterable[identifiers.ExperimentIdentifier]],
    attribute_filter: filters.AttributeFilter,
    batch_size: int,
) -> Generator[util.Page[AttributeDefinition], None, None]:
    params: dict[str, Any] = {
        "projectIdentifiers": list(project_identifiers),
        "attributeNameFilter": dict(),
        "nextPage": {"limit": batch_size},
    }

    if experiment_identifiers is not None:
        params["experimentIdsFilter"] = [str(e) for e in experiment_identifiers]

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
            {"attributeType": types.map_attribute_type_python_to_backend(_type)} for _type in attribute_types
        ]

    # note: attribute_filter.aggregations is intentionally ignored

    return util.fetch_pages(
        client=client,
        fetch_page=_fetch_attribute_definitions_page,
        process_page=_process_attribute_definitions_page,
        make_new_page_params=ft.partial(_make_new_attribute_definitions_page_params, batch_size=batch_size),
        params=params,
    )


def _fetch_attribute_definitions_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> QueryAttributeDefinitionsResultDTO:
    body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

    response = util.backoff_retry(
        query_attribute_definitions_within_project.sync_detailed,
        client=client,
        body=body,
    )

    return response.parsed


def _process_attribute_definitions_page(
    data: QueryAttributeDefinitionsResultDTO,
) -> util.Page[AttributeDefinition]:
    items = []
    for entry in data.entries:
        item = AttributeDefinition(
            name=entry.name,
            type=types.map_attribute_type_backend_to_python(str(entry.type)),
        )
        items.append(item)
    return util.Page(items=items)


def _make_new_attribute_definitions_page_params(
    params: dict[str, Any],
    data: Optional[QueryAttributeDefinitionsResultDTO],
    batch_size: int,
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.next_page.next_page_token
    if not next_page_token or len(data.entries) < batch_size:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params


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
