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
    Generator,
    Literal,
    Optional,
    Tuple,
)

import pandas as pd

from .. import identifiers
from ..client import get_client
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
    _BaseAttributeFilter,
    _Filter,
)
from ..identifiers import ProjectIdentifier
from ..output_format import create_series_dataframe
from ..retrieval import (
    search,
    series,
    split,
    util,
)
from ..retrieval.search import ContainerType

__all__ = ("fetch_series",)


def fetch_series(
    *,
    project_identifier: ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    include_time: Optional[Literal["absolute"]],
    step_range: Tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    context: Optional[Context] = None,
    container_type: ContainerType,
) -> pd.DataFrame:
    validation.validate_step_range(step_range)
    validation.validate_tail_limit(tail_limit)
    validation.validate_include_time(include_time)
    attributes_restricted = validation.restrict_attribute_filter_type(
        attributes, type_in={"string_series", "histogram_series", "file_series"}
    )

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
        inferred_filter = inference_result.get_result_or_raise()

        sys_id_label_mapping: dict[identifiers.SysId, str] = {}

        def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
            for page in search.fetch_sys_id_labels(container_type)(
                client=client,
                project_identifier=project_identifier,
                filter_=inferred_filter,
            ):
                sys_ids = []
                for item in page.items:
                    sys_id_label_mapping[item.sys_id] = item.label
                    sys_ids.append(item.sys_id)
                yield sys_ids

        output = concurrency.generate_concurrently(
            items=go_fetch_sys_attrs(),
            executor=executor,
            downstream=lambda sys_ids: _components.fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attributes_restricted,
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
                        ),
                    ),
                    executor=executor,
                    downstream=lambda run_attribute_definitions_split: concurrency.generate_concurrently(
                        items=series.fetch_series_values(
                            client=client,
                            run_attribute_definitions=run_attribute_definitions_split,
                            include_inherited=lineage_to_the_root,
                            step_range=step_range,
                            tail_limit=tail_limit,
                        ),
                        executor=executor,
                        downstream=concurrency.return_value,
                    ),
                ),
            ),
        )
        results: Generator[
            util.Page[tuple[identifiers.RunAttributeDefinition, list[series.SeriesValue]]], None, None
        ] = concurrency.gather_results(output)

        series_data: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]] = {}
        for result in results:
            for run_attribute_definition, series_values in result.items:
                series_data.setdefault(run_attribute_definition, []).extend(series_values)

        return create_series_dataframe(
            series_data,
            sys_id_label_mapping,
            index_column_name="experiment" if container_type == ContainerType.EXPERIMENT else "run",
            timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        )
