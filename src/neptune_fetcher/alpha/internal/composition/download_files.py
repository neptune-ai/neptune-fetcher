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
import os
import pathlib
from typing import (
    Generator,
    Optional,
)

import pandas as pd

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import (
    identifiers,
    output_format,
)
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.retrieval import (
    attribute_definitions,
    files,
    search,
)
from neptune_fetcher.alpha.internal.retrieval.search import ContainerType


def download_files(
    filter_: Optional[Filter],
    attributes: AttributeFilter,
    destination: pathlib.Path,
    context: Optional[Context],
    container_type: ContainerType,
) -> pd.DataFrame:
    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    _ensure_write_access(destination)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project,
            filter_=filter_,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        if "file" in attributes.type_in:
            attributes.type_in = ["file"]
        else:
            raise ValueError("Only file attributes are supported for file download.")

        sys_id_label_mapping: dict[identifiers.SysId, str] = {}

        def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
            for page in search.fetch_sys_id_labels(container_type)(
                client=client,
                project_identifier=project,
                filter_=filter_,
            ):
                sys_ids = []
                for item in page.items:
                    sys_id_label_mapping[item.sys_id] = item.label
                    sys_ids.append(item.sys_id)
                yield sys_ids

        output = concurrency.generate_concurrently(
            items=go_fetch_sys_attrs(),
            executor=executor,
            downstream=lambda sys_ids: _components.fetch_attribute_definition_aggregations_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids,
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=definitions_page.items,
                    downstream=lambda values_page: concurrency.generate_concurrently(
                        items=(
                            (value, file_)
                            for value, file_ in zip(
                                values_page.items,
                                files.fetch_signed_urls(
                                    client=client,
                                    project_identifier=project,
                                    file_paths=[value.value.path for value in values_page.items],
                                ),
                            )
                        ),
                        executor=executor,
                        downstream=lambda run_file_tuple: concurrency.return_value(
                            (
                                run_file_tuple[0].run_identifier,
                                run_file_tuple[0].attribute_definition,
                                files.download_file_retry(
                                    client=client,
                                    project_identifier=project,
                                    signed_file=run_file_tuple[1],
                                    target_path=files.create_target_path(
                                        destination=destination,
                                        experiment_name=sys_id_label_mapping[run_file_tuple[0].run_identifier.sys_id],
                                        attribute_path=run_file_tuple[0].attribute_definition.name,
                                    ),
                                ),
                            )
                        ),
                    ),
                ),
            ),
        )

        results: Generator[
            tuple[identifiers.RunIdentifier, attribute_definitions.AttributeDefinition, Optional[pathlib.Path]],
            None,
            None,
        ] = concurrency.gather_results(output)
        file_list = list(results)

        return output_format.create_files_dataframe(file_list, sys_id_label_mapping)


def _ensure_write_access(destination: pathlib.Path) -> None:
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    if not destination.is_dir():
        raise NotADirectoryError(f"Destination is not a directory: {destination}")

    if not os.access(destination, os.W_OK):
        raise PermissionError(f"No write access to the directory: {destination}")
