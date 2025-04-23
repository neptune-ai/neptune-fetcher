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
from collections import defaultdict
from typing import (
    Generator,
    Literal,
    Optional,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import context as _context
from neptune_fetcher.alpha.internal import (
    identifiers,
    output_format,
)
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.composition.attributes import AttributeDefinitionAggregation
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.alpha.internal.retrieval import attribute_values as att_vals
from neptune_fetcher.alpha.internal.retrieval import (
    search,
    util,
)

__all__ = ("fetch_table",)


def fetch_table(
    filter_: Optional[Filter],
    attributes: AttributeFilter,
    sort_by: Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: Optional[int],
    type_suffix_in_column_names: bool,
    context: Optional[_context.Context],
    container_type: search.ContainerType,
) -> pd.DataFrame:
    _validate_limit(limit)
    _sort_direction = _validate_sort_direction(sort_direction)

    valid_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

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

        type_inference.infer_attribute_types_in_sort_by(
            client=client,
            project_identifier=project,
            filter_=filter_,
            sort_by=sort_by,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        sys_id_label_mapping: dict[identifiers.SysId, str] = {}
        result_by_id: dict[identifiers.SysId, list[att_vals.AttributeValue]] = {}
        selected_aggregations: dict[att_defs.AttributeDefinition, set[str]] = defaultdict(set)

        def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
            for page in search.fetch_sys_id_labels(container_type)(
                client=client,
                project_identifier=project,
                filter_=filter_,
                sort_by=sort_by,
                sort_direction=_sort_direction,
                limit=limit,
            ):
                sys_ids = []
                for item in page.items:
                    result_by_id[item.sys_id] = []  # I assume that dict preserves the order set here
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
                downstream=lambda sys_ids_split, definitions_page, aggregations_page: concurrency.fork_concurrently(
                    executor=executor,
                    downstreams=[
                        lambda: _components.fetch_attribute_values_split(
                            client=client,
                            project_identifier=project,
                            executor=executor,
                            sys_ids=sys_ids_split,
                            attribute_definitions=definitions_page.items,
                            downstream=concurrency.return_value,
                        ),
                        lambda: concurrency.return_value(aggregations_page.items),
                    ],
                ),
            ),
        )
        results: Generator[
            Union[util.Page[att_vals.AttributeValue], dict[att_defs.AttributeDefinition, set[str]]], None, None
        ] = concurrency.gather_results(output)

        for result in results:
            if isinstance(result, util.Page):
                attribute_values_page = result
                for attribute_value in attribute_values_page.items:
                    sys_id = attribute_value.run_identifier.sys_id
                    result_by_id[sys_id].append(attribute_value)
            elif isinstance(result, list):
                aggregations: list[AttributeDefinitionAggregation] = result
                for aggregation in aggregations:
                    selected_aggregations[aggregation.attribute_definition].add(aggregation.aggregation)
            else:
                raise RuntimeError(f"Unexpected result type: {type(result)}")

    result_by_name = _map_keys_preserving_order(result_by_id, sys_id_label_mapping)
    dataframe = output_format.convert_table_to_dataframe(
        table_data=result_by_name,
        selected_aggregations=selected_aggregations,
        type_suffix_in_column_names=type_suffix_in_column_names,
        index_column_name="experiment" if container_type == search.ContainerType.EXPERIMENT else "run",
    )
    return dataframe


def _map_keys_preserving_order(
    result_by_id: dict[identifiers.SysId, list[att_vals.AttributeValue]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
) -> dict[str, list[att_vals.AttributeValue]]:
    result_by_name = {}
    for sys_id, values in result_by_id.items():
        label = sys_id_label_mapping[sys_id]
        result_by_name[label] = values
    return result_by_name


def _validate_limit(limit: Optional[int]) -> None:
    """Validate that limit is either None or a positive integer."""
    if limit is not None:
        if not isinstance(limit, int):
            raise ValueError("limit must be None or an integer")
        if limit <= 0:
            raise ValueError("limit must be greater than 0")


def _validate_sort_direction(sort_direction: Literal["asc", "desc"]) -> Literal["asc", "desc"]:
    """Validate that sort_direction is either 'asc' or 'desc'."""
    if sort_direction not in ("asc", "desc"):
        raise ValueError("sort_direction must be either 'asc' or 'desc'")
    return sort_direction
