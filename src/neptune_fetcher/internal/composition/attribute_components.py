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
from typing import (
    Callable,
    Optional,
)

from neptune_api.client import AuthenticatedClient

from .. import (
    filters,
    identifiers,
)
from ..composition import concurrency
from ..composition.attributes import (
    AttributeDefinitionAggregation,
    fetch_attribute_definition_aggregations,
    fetch_attribute_definitions,
)
from ..retrieval import attribute_values as att_vals
from ..retrieval import (
    search,
    split,
    util,
)


def fetch_attribute_definitions_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    attribute_filter: filters._BaseAttributeFilter,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    sys_ids: list[identifiers.SysId],
    downstream: Callable[[list[identifiers.SysId], util.Page[identifiers.AttributeDefinition]], concurrency.OUT],
) -> concurrency.OUT:
    return concurrency.generate_concurrently(
        items=split.split_sys_ids(sys_ids),
        executor=executor,
        downstream=lambda sys_ids_split: concurrency.generate_concurrently(
            fetch_attribute_definitions(
                client=client,
                project_identifiers=[project_identifier],
                run_identifiers=[identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split],
                attribute_filter=attribute_filter,
                executor=fetch_attribute_definitions_executor,
            ),
            executor=executor,
            downstream=lambda definitions: downstream(sys_ids_split, definitions),
        ),
    )


def fetch_attribute_definition_aggregations_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    attribute_filter: filters._BaseAttributeFilter,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    sys_ids: list[identifiers.SysId],
    downstream: Callable[
        [
            list[identifiers.SysId],
            util.Page[identifiers.AttributeDefinition],
            util.Page[AttributeDefinitionAggregation],
        ],
        concurrency.OUT,
    ],
) -> concurrency.OUT:
    return concurrency.generate_concurrently(
        items=split.split_sys_ids(sys_ids),
        executor=executor,
        downstream=lambda sys_ids_split: concurrency.generate_concurrently(
            fetch_attribute_definition_aggregations(
                client=client,
                project_identifiers=[project_identifier],
                run_identifiers=[identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split],
                attribute_filter=attribute_filter,
                executor=fetch_attribute_definitions_executor,
            ),
            executor=executor,
            downstream=lambda page_pair: downstream(sys_ids_split, page_pair[0], page_pair[1]),
        ),
    )


def fetch_attribute_definitions_complete(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    attribute_filter: filters._BaseAttributeFilter,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType,
    downstream: Callable[[util.Page[identifiers.AttributeDefinition]], concurrency.OUT],
) -> concurrency.OUT:
    if container_type == search.ContainerType.RUN and filter_ is None:
        return concurrency.generate_concurrently(
            fetch_attribute_definitions(
                client=client,
                project_identifiers=[project_identifier],
                run_identifiers=None,
                attribute_filter=attribute_filter,
                executor=fetch_attribute_definitions_executor,
            ),
            executor=executor,
            downstream=downstream,
        )
    else:
        return concurrency.generate_concurrently(
            items=search.fetch_sys_ids(
                client=client,
                project_identifier=project_identifier,
                filter_=filter_,
                container_type=container_type,
            ),
            executor=executor,
            downstream=lambda sys_ids_page: fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attribute_filter,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids_page.items,
                downstream=lambda _, definitions: downstream(definitions),
            ),
        )


def fetch_attribute_values_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    executor: Executor,
    sys_ids: list[identifiers.SysId],
    attribute_definitions: list[identifiers.AttributeDefinition],
    downstream: Callable[[util.Page[att_vals.AttributeValue]], concurrency.OUT],
) -> concurrency.OUT:
    return concurrency.generate_concurrently(
        items=split.split_sys_ids_attributes(sys_ids, attribute_definitions),
        executor=executor,
        downstream=lambda split_pair: concurrency.generate_concurrently(
            items=att_vals.fetch_attribute_values(
                client=client,
                project_identifier=project_identifier,
                run_identifiers=[identifiers.RunIdentifier(project_identifier, s) for s in split_pair[0]],
                attribute_definitions=split_pair[1],
            ),
            executor=executor,
            downstream=downstream,
        ),
    )
