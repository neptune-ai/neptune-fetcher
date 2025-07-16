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
    Generator,
    Literal,
    Optional,
)

import pandas as pd
from neptune_api.client import AuthenticatedClient

from .. import identifiers
from ..client import get_client
from ..composition import (
    concurrency,
    type_inference,
    validation,
)
from ..composition.attribute_components import fetch_attribute_definitions_split
from ..context import (
    Context,
    get_context,
    validate_context,
)
from ..filters import (
    _BaseAttributeFilter,
    _Filter,
)
from ..output_format import create_metrics_dataframe
from ..retrieval import (
    search,
    split,
)
from ..retrieval.metrics import (
    FloatPointValue,
    fetch_multiple_series_values,
)
from ..retrieval.search import ContainerType

__all__ = ("fetch_metrics",)


def fetch_metrics(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    include_time: Optional[Literal["absolute"]],
    step_range: tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    context: Optional[Context] = None,
    container_type: ContainerType,
) -> pd.DataFrame:
    validation.validate_step_range(step_range)
    validation.validate_tail_limit(tail_limit)
    validation.validate_include_time(include_time)
    restricted_attributes = validation.restrict_attribute_filter_type(attributes, type_in={"float_series"})

    valid_context = validate_context(context or get_context())
    client = get_client(context=valid_context)

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
            return create_metrics_dataframe(
                metrics_data={},
                sys_id_label_mapping={},
                index_column_name="experiment" if container_type == ContainerType.EXPERIMENT else "run",
                timestamp_column_name="absolute_time" if include_time == "absolute" else None,
                include_point_previews=include_point_previews,
                type_suffix_in_column_names=type_suffix_in_column_names,
            )
        inferred_filter = inference_result.get_result_or_raise()

        metrics_data, sys_id_to_label_mapping = _fetch_metrics(
            filter_=inferred_filter,
            attributes=restricted_attributes,
            client=client,
            project_identifier=project_identifier,
            step_range=step_range,
            lineage_to_the_root=lineage_to_the_root,
            include_point_previews=include_point_previews,
            tail_limit=tail_limit,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        df = create_metrics_dataframe(
            metrics_data=metrics_data,
            sys_id_label_mapping=sys_id_to_label_mapping,
            index_column_name="experiment" if container_type == ContainerType.EXPERIMENT else "run",
            timestamp_column_name="absolute_time" if include_time == "absolute" else None,
            include_point_previews=include_point_previews,
            type_suffix_in_column_names=type_suffix_in_column_names,
        )

    return df


def _fetch_metrics(
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    step_range: tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    include_point_previews: bool,
    tail_limit: Optional[int],
    container_type: ContainerType,
) -> tuple[dict[identifiers.RunAttributeDefinition, list[FloatPointValue]], dict[identifiers.SysId, str]]:
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
        downstream=lambda sys_ids: fetch_attribute_definitions_split(
            client=client,
            project_identifier=project_identifier,
            attribute_filter=attributes,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            sys_ids=sys_ids,
            downstream=lambda sys_ids_split, definitions_page: concurrency.generate_concurrently(
                items=split.split_series_attributes(
                    items=(
                        identifiers.RunAttributeDefinition(
                            run_identifier=identifiers.RunIdentifier(project_identifier, sys_id),
                            attribute_definition=definition,
                        )
                        for sys_id in sys_ids_split
                        for definition in definitions_page.items
                        if definition.type == "float_series"
                    )
                ),
                executor=executor,
                downstream=lambda run_attribute_definitions_split: concurrency.return_value(
                    fetch_multiple_series_values(
                        client=client,
                        run_attribute_definitions=run_attribute_definitions_split,
                        include_inherited=lineage_to_the_root,
                        include_preview=include_point_previews,
                        step_range=step_range,
                        tail_limit=tail_limit,
                    )
                ),
            ),
        ),
    )

    results: Generator[
        dict[identifiers.RunAttributeDefinition, list[FloatPointValue]], None, None
    ] = concurrency.gather_results(output)

    metrics_data: dict[identifiers.RunAttributeDefinition, list[FloatPointValue]] = {}
    for result in results:
        for run_attribute_definition, metric_points in result.items():
            metrics_data.setdefault(run_attribute_definition, []).extend(metric_points)

    return metrics_data, sys_id_label_mapping
