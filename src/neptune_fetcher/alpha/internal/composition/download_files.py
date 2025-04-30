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
import asyncio
import os
import pathlib
from typing import Optional

import pandas as pd
import requests
from azure.core.pipeline.transport._requests_asyncio import AsyncioRequestsTransport

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
from neptune_fetcher.alpha.internal.composition import type_inference
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

    type_inference.infer_attribute_types_in_filter(
        client=client,
        project_identifier=project,
        filter_=filter_,
        container_type=container_type,
    )

    if "file" in attributes.type_in:
        attributes.type_in = ["file"]
    else:
        raise ValueError("Only file attributes are supported for file download.")

    async def go(
        transport: AsyncioRequestsTransport,
    ) -> tuple[
        dict[identifiers.SysId, str],
        list[tuple[identifiers.RunIdentifier, attribute_definitions.AttributeDefinition, Optional[pathlib.Path]]],
    ]:
        sys_id_label_mapping: dict[identifiers.SysId, str] = {}
        file_list: list[
            tuple[identifiers.RunIdentifier, attribute_definitions.AttributeDefinition, Optional[pathlib.Path]]
        ] = []

        async for sys_ids_page in search.fetch_sys_id_labels(container_type)(
            client=client,
            project_identifier=project,
            filter_=filter_,
        ):
            sys_ids = []
            for item in sys_ids_page.items:
                sys_id_label_mapping[item.sys_id] = item.label
                sys_ids.append(item.sys_id)

            # todo: each split in parallel
            async for sys_ids_split, definitions_page in _components.fetch_attribute_definitions_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                sys_ids=sys_ids,
            ):
                # todo: each split in parallel
                async for values_page in _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    sys_ids=sys_ids_split,
                    attribute_definitions=definitions_page.items,
                ):
                    signed_files = await files.fetch_signed_urls(
                        client=client,
                        project_identifier=project,
                        file_paths=[value.value.path for value in values_page.items],
                    )

                    for file_value, signed_file in zip(values_page.items, signed_files):
                        # todo: each in parallel

                        target_path = await files.download_file_retry(
                            client=client,
                            project_identifier=project,
                            signed_file=signed_file,
                            target_path=files.create_target_path(
                                destination=destination,
                                experiment_name=sys_id_label_mapping[file_value.run_identifier.sys_id],
                                attribute_path=file_value.attribute_definition.name,
                            ),
                            transport=transport,
                        )
                        file_list.append((file_value.run_identifier, file_value.attribute_definition, target_path))

        return sys_id_label_mapping, file_list

    with requests.Session() as request_session, AsyncioRequestsTransport(
        session=request_session, session_owner=False
    ) as transport:
        sys_id_label_mapping, file_list = asyncio.run(go(transport))

    return output_format.create_files_dataframe(file_list, sys_id_label_mapping)


def _ensure_write_access(destination: pathlib.Path) -> None:
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    if not destination.is_dir():
        raise NotADirectoryError(f"Destination is not a directory: {destination}")

    if not os.access(destination, os.W_OK):
        raise PermissionError(f"No write access to the directory: {destination}")
