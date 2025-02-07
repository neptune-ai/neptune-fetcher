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

from concurrent.futures import Executor
from dataclasses import dataclass
from typing import (
    Generator,
    Iterable,
    Literal,
    Optional,
)

from neptune_api.client import AuthenticatedClient

import neptune_fetcher.alpha.filters as filters
from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
)
from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.alpha.internal.retrieval import util


@dataclass(frozen=True)
class AttributeDefinitionAggregation:
    attribute_definition: att_defs.AttributeDefinition
    aggregation: Optional[Literal["last", "min", "max", "average", "variance"]]


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters.BaseAttributeFilter,
    executor: Executor,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[att_defs.AttributeDefinition], None, None]:
    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, run_identifiers, attribute_filter, batch_size, executor
    )

    seen_items: set[att_defs.AttributeDefinition] = set()
    for page, _filter in pages_filters:
        new_items = [item for item in page.items if item not in seen_items]
        seen_items.update(new_items)
        yield util.Page(items=new_items)


def fetch_attribute_definition_aggregations(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Iterable[identifiers.RunIdentifier],
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
        client, project_identifiers, run_identifiers, attribute_filter, batch_size, executor
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
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters.BaseAttributeFilter,
    batch_size: int,
    executor: Executor,
) -> Generator[tuple[util.Page[att_defs.AttributeDefinition], filters.AttributeFilter], None, None]:
    def go_fetch_single(
        _filter: filters.AttributeFilter,
    ) -> Generator[util.Page[att_defs.AttributeDefinition], None, None]:
        return att_defs.fetch_attribute_definitions_single_filter(
            client=client,
            project_identifiers=project_identifiers,
            run_identifiers=run_identifiers,
            attribute_filter=_filter,
            batch_size=batch_size,
        )

    filters_ = att_defs.split_attribute_filters(attribute_filter)

    if len(filters_) == 1:
        head = filters_[0]
        for page in go_fetch_single(head):
            yield page, head
    else:
        output = concurrency.generate_concurrently(
            items=(_filter for _filter in filters_),
            executor=executor,
            downstream=lambda _filter: concurrency.generate_concurrently(
                items=go_fetch_single(_filter),
                executor=executor,
                downstream=lambda _page: concurrency.return_value((_page, _filter)),
            ),
        )
        yield from concurrency.gather_results(output)
