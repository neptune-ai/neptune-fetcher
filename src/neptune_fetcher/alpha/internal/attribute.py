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

import concurrent
import concurrent.futures
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
    Tuple,
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
    AttributeValue,
    extract_value,
    map_attribute_type_python_to_backend,
)


@dataclass(frozen=True)
class AttributeDefinition:
    name: str
    type: str
    source_filter: filter.AttributeFilter

    def key(self) -> Tuple[str, str]:
        return self.name, self.type


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    experiment_identifiers: Iterable[identifiers.ExperimentIdentifier],
    attribute_filter: filter.BaseAttributeFilter,
    batch_size: int = env.NEPTUNE_FETCHER_BATCH_SIZE.get(),
    executor: Optional[Executor] = None,
) -> Generator[util.Page[AttributeDefinition], None, None]:
    def split_to_tasks(
        _attribute_filter: filter.BaseAttributeFilter,
    ) -> List[filter.AttributeFilter]:
        if isinstance(_attribute_filter, filter.AttributeFilter):
            return [_attribute_filter]
        elif isinstance(_attribute_filter, filter._AttributeFilterAlternative):
            return list(it.chain.from_iterable(split_to_tasks(child) for child in _attribute_filter.filters))
        else:
            raise ValueError(f"Unexpected filter type: {type(_attribute_filter)}")

    def _main(_executor: Executor) -> Generator[util.Page[AttributeDefinition], None, None]:
        def _next(
            _generator: Generator[util.Page[AttributeDefinition], None, None]
        ) -> Tuple[util.Page[AttributeDefinition], Generator[util.Page[AttributeDefinition], None, None]]:
            return next(_generator, util.Page(items=[])), _generator

        def _go_fetch_single(
            _filter: filter.AttributeFilter,
        ) -> Tuple[util.Page[AttributeDefinition], Generator[util.Page[AttributeDefinition], None, None]]:
            _generator = _fetch_attribute_definitions_single_filter(
                client=client,
                project_identifiers=project_identifiers,
                experiment_identifiers=experiment_identifiers,
                attribute_filter=_filter,
                batch_size=batch_size,
            )
            return _next(_generator)

        filters = split_to_tasks(attribute_filter)
        returned_items: set[tuple[str, str]] = set()
        futures = [_executor.submit(_go_fetch_single, _filter) for _filter in filters]
        while futures:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = list(not_done)
            for f in done:
                page, generator = f.result()
                if page.items:
                    futures.append(_executor.submit(_next, generator))
                    page_items = [item for item in page.items if item.key() not in returned_items]
                    returned_items.update(item.key() for item in page_items)
                    if page_items:
                        yield util.Page(items=page_items)

    if executor is None:
        with util.create_executor() as _executor:
            yield from _main(_executor)
    else:
        yield from _main(executor)


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
        process_page=ft.partial(_process_attribute_definitions_page, source_filter=attribute_filter),
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
    source_filter: filter.AttributeFilter,
) -> util.Page[AttributeDefinition]:
    items = []
    for entry in data.entries:
        item = AttributeDefinition(
            name=entry.name,
            type=types.map_attribute_type_backend_to_python(str(entry.type)),
            source_filter=source_filter,
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
    batch_size: int = env.NEPTUNE_FETCHER_BATCH_SIZE.get(),
    executor: Optional[Executor] = None,
) -> Generator[util.Page[AttributeValue], None, None]:
    params: dict[str, Any] = {
        "experimentIdsFilter": list(str(e) for e in experiment_identifiers),
        "attributeNamesFilter": [ad.name for ad in attribute_definitions],
        "nextPage": {"limit": batch_size},
    }

    attribute_definitions_map = {
        (ad.name, map_attribute_type_python_to_backend(ad.type)): ad for ad in attribute_definitions
    }

    return util.fetch_pages(
        client=client,
        fetch_page=ft.partial(_fetch_attributes_page, project_identifier=project_identifier),
        process_page=ft.partial(
            _process_attributes_page,
            attribute_definitions_map=attribute_definitions_map,
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
    attribute_definitions_map: dict[Tuple[str, str], AttributeDefinition],
    project_identifier: identifiers.ProjectIdentifier,
) -> util.Page[AttributeValue]:
    items = []
    for entry in data.entries:
        experiment_identifier = identifiers.ExperimentIdentifier(
            project_identifier=project_identifier, sys_id=identifiers.SysId(entry.experimentShortId)
        )
        for attr in entry.attributes:
            attr_definition = attribute_definitions_map.get((attr.name, attr.type))
            if attr_definition is None:
                continue
            item = extract_value(attr, experiment_identifier=experiment_identifier)
            if item is None:
                continue
            if item.type == "float_series":
                item = AttributeValue(
                    name=item.name,
                    type=item.type,
                    value=_filter_aggregates(item.value, attr_definition.source_filter),
                    experiment_identifier=item.experiment_identifier,
                )
            items.append(item)
    return util.Page(items=items)


def _filter_aggregates(
    aggregates: types.FloatSeriesAggregates,
    attribute_filter: filter.AttributeFilter,
) -> types.FloatSeriesAggregatesSubset:
    return types.FloatSeriesAggregatesSubset(
        last=aggregates.last if "last" in attribute_filter.aggregations else None,
        min=aggregates.min if "min" in attribute_filter.aggregations else None,
        max=aggregates.max if "max" in attribute_filter.aggregations else None,
        average=aggregates.average if "average" in attribute_filter.aggregations else None,
        variance=aggregates.variance if "variance" in attribute_filter.aggregations else None,
    )


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
