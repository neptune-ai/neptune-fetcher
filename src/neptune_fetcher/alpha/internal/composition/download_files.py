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
    Tuple,
)

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import identifiers
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
from neptune_fetcher.alpha.internal.identifiers import SysId
from neptune_fetcher.alpha.internal.retrieval import (
    attribute_definitions,
    files,
    search,
    util,
)
from neptune_fetcher.alpha.internal.retrieval.search import ContainerType

SysIdLabel = Tuple[SysId, str]


def download_files(
    filter_: Optional[Filter],
    attributes: AttributeFilter,
    destination: pathlib.Path,
    context: Optional[Context],
    container_type: ContainerType,
) -> None:
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

        def process_sys_attrs_page(sys_attrs_page: util.Page[SysIdLabel]) -> concurrency.OUT:
            sys_id_to_label = {sys_id: label for sys_id, label in sys_attrs_page.items}
            return _components.fetch_attribute_definition_aggregations_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=[sys_id for sys_id, _ in sys_attrs_page.items],
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=_filter_file_refs(definitions_page.items),
                    downstream=lambda values_page: concurrency.generate_concurrently(
                        items=(
                            (value.run_identifier, file_)
                            for value, file_ in zip(
                                values_page.items,
                                files.fetch_signed_urls(
                                    client=client,
                                    project_identifier=project,
                                    file_paths=[value.value for value in values_page.items],
                                ),
                            )
                        ),
                        executor=executor,
                        downstream=lambda run_file_tuple: concurrency.return_value(
                            files.download_file(
                                signed_url=run_file_tuple[1].url,
                                target_path=_create_target_path(
                                    destination=destination,
                                    experiment_name=sys_id_to_label[run_file_tuple[0].sys_id],
                                    attribute_path=run_file_tuple[1].path,
                                ),  # type: ignore
                            )
                        ),
                    ),
                ),
            )

        if container_type == search.ContainerType.EXPERIMENT:
            experiment_pages = search.fetch_experiment_sys_attrs(
                client=client,
                project_identifier=project,
                filter_=filter_,
            )
            sys_id_label_pages: Generator[util.Page[SysIdLabel], None, None] = (
                util.Page(items=[(item.sys_id, item.sys_name) for item in page.items]) for page in experiment_pages
            )
        else:
            run_pages = search.fetch_run_sys_attrs(
                client=client,
                project_identifier=project,
                filter_=filter_,
            )
            sys_id_label_pages = (
                util.Page(items=[(item.sys_id, item.sys_custom_run_id) for item in page.items]) for page in run_pages
            )

        output = concurrency.generate_concurrently(
            items=sys_id_label_pages,
            executor=executor,
            downstream=process_sys_attrs_page,
        )

        results: Generator[None, None, None] = concurrency.gather_results(output)
        list(results)


def _ensure_write_access(destination: pathlib.Path) -> None:
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    if not os.access(destination, os.W_OK):
        raise PermissionError(f"No write access to the directory: {destination}")


def _filter_file_refs(
    definitions: list[attribute_definitions.AttributeDefinition],
) -> list[attribute_definitions.AttributeDefinition]:
    return [attribute for attribute in definitions if attribute.type == "file"]


def _create_target_path(destination: pathlib.Path, experiment_name: str, attribute_path: str) -> pathlib.Path:
    return destination / experiment_name / attribute_path
