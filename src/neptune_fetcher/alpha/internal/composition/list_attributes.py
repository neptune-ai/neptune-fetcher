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
#

__all__ = ("list_attributes",)

import asyncio
from typing import Optional

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import type_inference
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.retrieval import search


def list_attributes(
    filter_: Optional[Filter],
    attributes: AttributeFilter,
    context: Optional[Context],
    container_type: search.ContainerType,
) -> list[str]:
    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    project_identifier = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    type_inference.infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_,
        container_type=container_type,
    )

    async def go() -> list[str]:
        names = set()

        async for attr_def_page in _components.fetch_attribute_definitions_complete(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            attribute_filter=attributes,
            container_type=container_type,
        ):
            for attr_def in attr_def_page.items:
                names.add(attr_def.name)

        return sorted(names)

    return asyncio.run(go())
