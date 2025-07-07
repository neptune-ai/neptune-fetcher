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

from typing import (
    Generator,
    Optional,
)

from neptune_fetcher.internal import client as _client
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.composition import attribute_components as _components
from neptune_fetcher.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.internal.filters import (
    _AttributeFilter,
    _Filter,
)
from neptune_fetcher.internal.retrieval import (
    search,
    util,
)


def list_attributes(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _AttributeFilter,
    context: Optional[Context] = None,
    container_type: search.ContainerType,
) -> list[str]:
    valid_context = validate_context(context or get_context())
    client = _client.get_client(context=valid_context)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        inference_result = type_inference.infer_attribute_types_in_filter(
            client,
            project_identifier,
            filter_,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )
        if inference_result.is_run_domain_empty():
            return []
        filter_ = inference_result.get_result_or_raise()

        output = _components.fetch_attribute_definitions_complete(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            attribute_filter=attributes,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            downstream=concurrency.return_value,
            container_type=container_type,
        )

        results: Generator[util.Page[identifiers.AttributeDefinition], None, None] = concurrency.gather_results(output)
        names = set()
        for page in results:
            for item in page.items:
                names.add(item.name)

        return sorted(names)
