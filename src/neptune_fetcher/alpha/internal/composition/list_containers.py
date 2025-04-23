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
from typing import Optional

from neptune_fetcher.alpha.filters import Filter
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import context as _context
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.retrieval import search

__all__ = ("list_containers",)


def list_containers(
    filter_: Optional[Filter],
    context: Optional[_context.Context],
    container_type: search.ContainerType,
) -> list[str]:
    validated_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(validated_context)
    project_identifier = identifiers.ProjectIdentifier(validated_context.project)  # type: ignore

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        sys_attr_pages = search.fetch_sys_id_labels(container_type)(client, project_identifier, filter_)
        return list(sorted(attrs.label for page in sys_attr_pages for attrs in page.items))
