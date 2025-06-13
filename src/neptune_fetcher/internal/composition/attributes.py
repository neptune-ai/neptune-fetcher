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

import neptune_fetcher.internal.filters as filters
from neptune_fetcher.internal import (
    env,
    identifiers,
)
from neptune_fetcher.internal.composition import concurrency
from neptune_fetcher.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.internal.retrieval import util
from neptune_fetcher.internal.retrieval.attribute_types import TYPE_AGGREGATIONS


@dataclass(frozen=True)
class AttributeDefinitionAggregation:
    attribute_definition: identifiers.AttributeDefinition
    aggregation: Literal["last", "min", "max", "average", "variance"]


def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._BaseAttributeFilter,
    executor: Executor,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[identifiers.AttributeDefinition], None, None]:
    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, run_identifiers, attribute_filter, batch_size, executor
    )

    seen_items: set[identifiers.AttributeDefinition] = set()
    for page, filter_ in pages_filters:
        new_items = [item for item in page.items if item not in seen_items]
        seen_items.update(new_items)
        yield util.Page(items=new_items)


def fetch_attribute_definition_aggregations(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Iterable[identifiers.RunIdentifier],
    attribute_filter: filters._BaseAttributeFilter,
    executor: Executor,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[
    tuple[util.Page[identifiers.AttributeDefinition], util.Page[AttributeDefinitionAggregation]], None, None
]:
    """
    Each attribute definition is yielded once when it's first encountered.
    If the attribute definition is of a type that supports aggregations (for now only float_series),
    it's then yielded once for each aggregation in the filter that returned it.
    """

    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, run_identifiers, attribute_filter, batch_size, executor
    )

    seen_definitions: set[identifiers.AttributeDefinition] = set()
    seen_definition_aggregations: set[AttributeDefinitionAggregation] = set()

    for page, filter_ in pages_filters:
        new_definitions = []
        new_definition_aggregations = []

        for definition in page.items:
            if definition not in seen_definitions:
                new_definitions.append(definition)
                seen_definitions.add(definition)

            if definition.type in TYPE_AGGREGATIONS.keys():
                for aggregation in filter_.aggregations:
                    if aggregation not in TYPE_AGGREGATIONS[definition.type]:
                        continue

                    definition_aggregation = AttributeDefinitionAggregation(
                        attribute_definition=definition, aggregation=aggregation
                    )
                    if definition_aggregation not in seen_definition_aggregations:
                        new_definition_aggregations.append(definition_aggregation)
                        seen_definition_aggregations.add(definition_aggregation)

        yield util.Page(items=new_definitions), util.Page(items=new_definition_aggregations)


def _fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._BaseAttributeFilter,
    batch_size: int,
    executor: Executor,
) -> Generator[tuple[util.Page[identifiers.AttributeDefinition], filters._AttributeFilter], None, None]:
    def go_fetch_single(
        filter_: filters._AttributeFilter,
    ) -> Generator[util.Page[identifiers.AttributeDefinition], None, None]:
        return att_defs.fetch_attribute_definitions_single_filter(
            client=client,
            project_identifiers=project_identifiers,
            run_identifiers=run_identifiers,
            attribute_filter=filter_,
            batch_size=batch_size,
        )

    filters_ = att_defs.split_attribute_filters(attribute_filter)

    if len(filters_) == 1:
        head = filters_[0]
        for page in go_fetch_single(head):
            yield page, head
    else:
        output = concurrency.generate_concurrently(
            items=(filter_ for filter_ in filters_),
            executor=executor,
            downstream=lambda filter_: concurrency.generate_concurrently(
                items=go_fetch_single(filter_),
                executor=executor,
                downstream=lambda _page: concurrency.return_value((_page, filter_)),
            ),
        )
        yield from concurrency.gather_results(output)
