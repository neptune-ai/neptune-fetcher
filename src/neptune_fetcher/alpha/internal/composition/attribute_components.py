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

from typing import (
    AsyncGenerator,
    Optional,
)

from neptune_api.client import AuthenticatedClient

import neptune_fetcher.alpha.filters as filters
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition.attributes import (
    AttributeDefinitionAggregation,
    fetch_attribute_definition_aggregations,
    fetch_attribute_definitions,
)
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.alpha.internal.retrieval import attribute_values as att_vals
from neptune_fetcher.alpha.internal.retrieval import (
    search,
    split,
    util,
)


async def fetch_attribute_definitions_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    attribute_filter: filters.BaseAttributeFilter,
    sys_ids: list[identifiers.SysId],
) -> AsyncGenerator[tuple[list[identifiers.SysId], util.Page[att_defs.AttributeDefinition]], None]:
    for sys_ids_split in split.split_sys_ids(sys_ids):
        # todo: each split in parallel
        async for attr_defs_page in fetch_attribute_definitions(
            client=client,
            project_identifiers=[project_identifier],
            run_identifiers=[identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split],
            attribute_filter=attribute_filter,
        ):
            yield (sys_ids_split, attr_defs_page)


async def fetch_attribute_definition_aggregations_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    attribute_filter: filters.BaseAttributeFilter,
    sys_ids: list[identifiers.SysId],
) -> AsyncGenerator[
    tuple[list[identifiers.SysId], util.Page[att_defs.AttributeDefinition], util.Page[AttributeDefinitionAggregation]],
    None,
]:
    for sys_ids_split in split.split_sys_ids(sys_ids):
        # todo: each split in parallel
        async for attr_defs_page, attr_aggs_page in fetch_attribute_definition_aggregations(
            client=client,
            project_identifiers=[project_identifier],
            run_identifiers=[identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split],
            attribute_filter=attribute_filter,
        ):
            yield sys_ids_split, attr_defs_page, attr_aggs_page


async def fetch_attribute_definitions_complete(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters.Filter],
    attribute_filter: filters.BaseAttributeFilter,
    container_type: search.ContainerType,
) -> AsyncGenerator[util.Page[att_defs.AttributeDefinition], None]:
    if container_type == search.ContainerType.RUN and filter_ is None:
        async for attr_defs_page in fetch_attribute_definitions(
            client=client,
            project_identifiers=[project_identifier],
            run_identifiers=None,
            attribute_filter=attribute_filter,
        ):
            yield attr_defs_page
    else:
        async for sys_ids_page in search.fetch_experiment_sys_ids(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            container_type=container_type,
        ):
            async for _, attr_defs_page in fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attribute_filter,
                sys_ids=sys_ids_page.items,
            ):
                yield attr_defs_page


async def fetch_attribute_values_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    sys_ids: list[identifiers.SysId],
    attribute_definitions: list[att_defs.AttributeDefinition],
) -> AsyncGenerator[util.Page[att_vals.AttributeValue], None]:
    for sys_ids_split, attr_defs_split in split.split_sys_ids_attributes(sys_ids, attribute_definitions):
        # todo: each split in parallel
        async for values_page in att_vals.fetch_attribute_values(
            client=client,
            project_identifier=project_identifier,
            run_identifiers=[identifiers.RunIdentifier(project_identifier, s) for s in sys_ids_split],
            attribute_definitions=attr_defs_split,
        ):
            yield values_page
