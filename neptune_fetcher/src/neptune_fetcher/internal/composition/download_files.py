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
)

import pandas as pd

from .. import client as _client
from .. import (
    identifiers,
    output_format,
)
from ..composition import attribute_components as _components
from ..composition import (
    concurrency,
    type_inference,
    validation,
)
from ..context import (
    Context,
    get_context,
    validate_context,
)
from ..filters import (
    _AttributeFilter,
    _Filter,
)
from ..retrieval import (
    files,
    search,
)
from ..retrieval.search import ContainerType


def download_files(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _AttributeFilter,
    destination: pathlib.Path,
    context: Optional[Context],
    container_type: ContainerType,
) -> pd.DataFrame:
    valid_context = validate_context(context or get_context())
    client = _client.get_client(context=valid_context)

    attributes_restricted = validation.restrict_attribute_filter_type(attributes, type_in={"file"})
    validation.ensure_write_access(destination)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        inference_result = type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )
        if inference_result.is_run_domain_empty():
            return output_format.create_files_dataframe(
                [],
                {},
                index_column_name="experiment" if container_type == search.ContainerType.EXPERIMENT else "run",
            )
        filter_ = inference_result.get_result_or_raise()

        sys_id_label_mapping: dict[identifiers.SysId, str] = {}

        def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
            for page in search.fetch_sys_id_labels(container_type)(
                client=client,
                project_identifier=project_identifier,
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
                project_identifier=project_identifier,
                attribute_filter=attributes_restricted,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids,
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project_identifier,
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
                                    project_identifier=project_identifier,
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
                                    project_identifier=project_identifier,
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
            tuple[identifiers.RunIdentifier, identifiers.AttributeDefinition, Optional[pathlib.Path]],
            None,
            None,
        ] = concurrency.gather_results(output)
        file_list = list(results)

        return output_format.create_files_dataframe(
            file_list,
            sys_id_label_mapping,
            index_column_name="experiment" if container_type == search.ContainerType.EXPERIMENT else "run",
        )
