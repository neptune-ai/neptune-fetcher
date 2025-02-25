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
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.client import get_client
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
    search,
    series,
    util,
)
from neptune_fetcher.alpha.internal.retrieval.search import ContainerType

__all__ = (
    "fetch_experiment_series",
    "fetch_run_series",
)


_PATHS_PER_BATCH: int = 10_000


def fetch_experiment_series(
    experiments: Union[str, Filter],
    attributes: Union[str, AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=attributes, type_in=["float_series"])

    return _fetch_series(
        filter_=experiments,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        context=context,
        container_type=ContainerType.EXPERIMENT,
    )


def fetch_run_series(
    runs: Union[str, Filter],
    attributes: Union[str, AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    if isinstance(runs, str):
        runs = Filter.matches_all(Attribute("sys/custom_run_id", type="string"), regex=runs)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=attributes, type_in=["float_series"])

    return _fetch_series(
        filter_=runs,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        context=context,
        container_type=ContainerType.RUN,
    )


def _fetch_series(
    filter_: Filter,
    attributes: AttributeFilter,
    include_time: Optional[Literal["absolute"]],
    step_range: Tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    context: Optional[Context],
    container_type: ContainerType,
) -> pd.DataFrame:
    _validate_step_range(step_range)
    _validate_tail_limit(tail_limit)
    _validate_include_time(include_time)

    valid_context = validate_context(context or get_context())
    client = get_client(valid_context)
    project_identifier = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

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
            container_type=container_type,
        )

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
            downstream=lambda sys_ids: _components.fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids,
                downstream=lambda sys_ids_split, definitions_page: concurrency.generate_concurrently(
                    items=series.fetch_series_values(
                        client=client,
                        run_attribute_definitions=[
                            series.RunAttributeDefinition(
                                run_identifier=identifiers.RunIdentifier(project_identifier, sys_id),
                                attribute_definition=definition,
                            )
                            for sys_id in sys_ids_split
                            for definition in _filter_series(definitions_page.items)
                        ],
                        include_inherited=lineage_to_the_root,
                        step_range=step_range,
                        tail_limit=tail_limit,
                    ),
                    executor=executor,
                    downstream=concurrency.return_value,
                ),
            ),
        )
        results: Generator[
            util.Page[tuple[series.RunAttributeDefinition, list[series.StringSeriesValue]]], None, None
        ] = concurrency.gather_results(output)

        series_data: dict[series.RunAttributeDefinition, list[series.StringSeriesValue]] = {}
        for result in results:
            for run_attribute_definition, series_values in result.items:
                series_data.setdefault(run_attribute_definition, []).extend(series_values)

        # TODO
        # index_column_name = "experiment" if container_type == ContainerType.EXPERIMENT else "run"
        print(series_data)
        print(sys_id_label_mapping)

    return pd.DataFrame()


# TODO: common validation and output of series+metrics
def _validate_include_time(include_time: Optional[Literal["absolute"]]) -> None:
    if include_time is not None:
        if include_time not in ["absolute"]:
            raise ValueError("include_time must be 'absolute'")


def _validate_step_range(step_range: Tuple[Optional[float], Optional[float]]) -> None:
    """Validate that a step range tuple contains valid values and is properly ordered."""
    if not isinstance(step_range, tuple) or len(step_range) != 2:
        raise ValueError("step_range must be a tuple of two values")

    start, end = step_range

    # Validate types
    if start is not None and not isinstance(start, (int, float)):
        raise ValueError("step_range start must be None or a number")
    if end is not None and not isinstance(end, (int, float)):
        raise ValueError("step_range end must be None or a number")

    # Validate range order if both values are provided
    if start is not None and end is not None and start > end:
        raise ValueError("step_range start must be less than or equal to end")


def _validate_tail_limit(tail_limit: Optional[int]) -> None:
    """Validate that tail_limit is either None or a positive integer."""
    if tail_limit is not None:
        if not isinstance(tail_limit, int):
            raise ValueError("tail_limit must be None or an integer")
        if tail_limit <= 0:
            raise ValueError("tail_limit must be greater than 0")


def _filter_series(
    definitions: list[attribute_definitions.AttributeDefinition],
) -> list[attribute_definitions.AttributeDefinition]:
    return [attribute for attribute in definitions if attribute.type == "string_series"]
