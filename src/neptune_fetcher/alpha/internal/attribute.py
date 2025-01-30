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
    Optional,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import (
    query_attribute_definitions_within_project,
    query_attributes_within_project_proto,
)
from neptune_retrieval_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
    QueryAttributesBodyDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.attributes_pb2 import ProtoQueryAttributesResultDTO

from neptune_fetcher.alpha import filter
from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
    types,
    util,
)

__ALL__ = ("find_attribute_definitions", "stream_attribute_definitions")

from neptune_fetcher.alpha.internal.types import (
    extract_value,
    map_attribute_type_backend_to_python,
)


@dataclass(frozen=True)
class AttributeDefinition:
    name: str
    type: str


@dataclass(frozen=True)
class AttributeValue:
    attribute_definition: AttributeDefinition
    value: Any
    experiment_identifier: identifiers.ExperimentIdentifier


def _split_to_tasks(
    _attribute_filter: filter.BaseAttributeFilter,
) -> List[filter.AttributeFilter]:
    if isinstance(_attribute_filter, filter.AttributeFilter):
        return [_attribute_filter]
    elif isinstance(_attribute_filter, filter._AttributeFilterAlternative):
        return list(it.chain.from_iterable(_split_to_tasks(child) for child in _attribute_filter.filters))
    else:
        raise ValueError(f"Unexpected filter type: {type(_attribute_filter)}")


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filter.BaseAttributeFilter,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
    executor: Optional[Executor] = None,
) -> Generator[util.Page[AttributeDefinition], None, None]:
    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, experiment_identifiers, attribute_filter, batch_size, executor
    )

    seen_items: set[AttributeDefinition] = set()
    for page, _filter in pages_filters:
        definitions = [item for item in page.items if item not in seen_items]
        seen_items.update(definitions)
        yield util.Page(items=definitions)


# @dataclass(frozen=True)
# class AttributeDefinitionAndAggregation:
#     attribute_definition: AttributeDefinition
#     aggregations: Optional[Literal["last", "min", "max", "average", "variance"]] = None


# def fetch_attribute_definitions_and_aggregations(
#         client: AuthenticatedClient,
#         project_identifiers: Iterable[identifiers.ProjectIdentifier],
#         experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
#         attribute_filter: filter.BaseAttributeFilter,
#         batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
#         executor: Optional[Executor] = None,
# ) -> Generator[util.Page[AttributeDefinitionAndAggregations], None, None]:
#     pages_filters = _fetch_attribute_definitions(
#         client, project_identifiers, experiment_identifiers, attribute_filter, batch_size, executor
#     )
#
#     seen_items: set[AttributeDefinition] = set()
#     seen_items_aggregations: dict[AttributeDefinition, set[Literal["last", "min", "max", "average", "variance"]]] = {}
#
#     for page, _filter in pages_filters:
#
#         definitions = [item for item in page.items if item not in seen_items]
#         seen_items.update(definitions)
#         yield util.Page(items=definitions)


def _fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filter.BaseAttributeFilter,
    batch_size: int,
    executor: Optional[Executor],
) -> Generator[tuple[util.Page[AttributeDefinition], filter.AttributeFilter], None, None]:
    def use_executor(
        _executor: Executor,
    ) -> Generator[tuple[util.Page[AttributeDefinition], filter.AttributeFilter], None, None]:
        def go_fetch_single(_filter: filter.AttributeFilter) -> Generator[util.Page[AttributeDefinition], None, None]:
            return _fetch_attribute_definitions_single_filter(
                client=client,
                project_identifiers=project_identifiers,
                experiment_identifiers=experiment_identifiers,
                attribute_filter=_filter,
                batch_size=batch_size,
            )

        filters = _split_to_tasks(attribute_filter)
        output = util.process_concurrently(
            items=(_filter for _filter in filters),
            executor=_executor,
            downstream=lambda _filter: util.process_concurrently(
                items=go_fetch_single(_filter),
                executor=_executor,
                downstream=lambda page: util.return_value((page, _filter)),
            ),
        )
        yield from util.gather_results(output)

    if executor is None:
        with util.create_thread_pool_executor() as _executor:
            yield from use_executor(_executor)
    else:
        yield from use_executor(executor)


def _fetch_attribute_definitions_single_filter(
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
            {"attributeType": types.map_attribute_type_python_to_backend(_type)} for _type in attribute_types
        ]

    # note: attribute_filter.aggregations is intentionally ignored

    return util.fetch_pages(
        client=client,
        fetch_page=_fetch_attribute_definitions_page,
        process_page=_process_attribute_definitions_page,
        make_new_page_params=_make_new_attribute_definitions_page_params,
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
    params: dict[str, Any], data: Optional[QueryAttributeDefinitionsResultDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.next_page.next_page_token
    if not next_page_token:
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


def fetch_attribute_values(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_definitions: Iterable[AttributeDefinition],
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.get(),
    executor: Optional[Executor] = None,
) -> Generator[util.Page[AttributeValue], None, None]:
    params: dict[str, Any] = {
        "experimentIdsFilter": list(str(e) for e in experiment_identifiers),
        "attributeNamesFilter": [ad.name for ad in attribute_definitions],
        "nextPage": {"limit": batch_size},
    }

    attribute_definitions_set: set[AttributeDefinition] = set(attribute_definitions)

    return util.fetch_pages(
        client=client,
        fetch_page=ft.partial(_fetch_attributes_page, project_identifier=project_identifier),
        process_page=ft.partial(
            _process_attributes_page,
            attribute_definitions_set=attribute_definitions_set,
            project_identifier=project_identifier,
        ),
        make_new_page_params=_make_new_attributes_page_params,
        params=params,
        executor=executor,
    )


def _fetch_attributes_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project_identifier: identifiers.ProjectIdentifier,
) -> ProtoQueryAttributesResultDTO:
    body = QueryAttributesBodyDTO.from_dict(params)

    response = util.backoff_retry(
        query_attributes_within_project_proto.sync_detailed,
        client=client,
        body=body,
        project_identifier=project_identifier,
    )

    return ProtoQueryAttributesResultDTO.FromString(response.content)


def _process_attributes_page(
    data: ProtoQueryAttributesResultDTO,
    attribute_definitions_set: set[AttributeDefinition],
    project_identifier: identifiers.ProjectIdentifier,
) -> util.Page[AttributeValue]:
    items = []
    for entry in data.entries:
        experiment_identifier = identifiers.ExperimentIdentifier(
            project_identifier=project_identifier, sys_id=identifiers.SysId(entry.experimentShortId)
        )

        for attr in entry.attributes:
            attr_definition = AttributeDefinition(name=attr.name, type=map_attribute_type_backend_to_python(attr.type))
            if attr_definition not in attribute_definitions_set:
                continue

            item_value = extract_value(attr)
            if item_value is None:
                continue

            attr_value = AttributeValue(
                attribute_definition=attr_definition,
                value=item_value,
                experiment_identifier=experiment_identifier,
            )
            items.append(attr_value)

    return util.Page(items=items)


def _make_new_attributes_page_params(
    params: dict[str, Any], data: Optional[ProtoQueryAttributesResultDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.nextPage.nextPageToken
    if not next_page_token:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params
