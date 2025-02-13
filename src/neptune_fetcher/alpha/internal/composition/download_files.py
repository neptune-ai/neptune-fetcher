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

import pathlib
from typing import (
    Generator,
    Optional,
    Union,
)

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.retrieval import (
    files,
    search,
)


def download_files(
    experiments: Optional[Filter],
    attributes: AttributeFilter,
    destination: Optional[str],
    context: Optional[Context],
) -> None:
    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    destination_path = pathlib.Path(destination or ".").resolve()
    print(destination_path)
    # destination_path.mkdir(parents=True, exist_ok=True)
    # TODO: check write permission before starting download

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        # TODO: type inference. special case for file_ref?

        output = concurrency.generate_concurrently(
            items=search.fetch_experiment_sys_ids(
                client=client,
                project_identifier=project,
                filter_=experiments,
            ),
            executor=executor,
            downstream=lambda sys_ids_page: _components.fetch_attribute_definition_aggregations_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids_page.items,
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=definitions_page.items,
                    downstream=lambda values_page: concurrency.generate_concurrently(
                        items=(
                            f
                            for f in files.fetch_signed_urls(
                                client=client,
                                project_identifier=project,
                                file_paths=[value.value for value in values_page.items],
                            )
                        ),
                        executor=executor,
                        downstream=concurrency.return_value,
                    ),
                ),
            ),
        )

        results: Generator[files.SignedFile, None, None] = concurrency.gather_results(output)
        print(list(results))  # TODO ofc
