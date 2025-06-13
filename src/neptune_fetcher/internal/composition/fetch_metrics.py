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

import concurrent
import itertools as it
from concurrent.futures import Executor
from typing import (
    Generator,
    Iterable,
    Literal,
    Optional,
)

import pandas as pd
from neptune_api.client import AuthenticatedClient

from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.client import get_client
from neptune_fetcher.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.internal.composition.attributes import fetch_attribute_definitions
from neptune_fetcher.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.internal.filters import (
    _AttributeFilter,
    _Filter,
)
from neptune_fetcher.internal.identifiers import RunIdentifier
from neptune_fetcher.internal.output_format import create_metrics_dataframe
from neptune_fetcher.internal.retrieval import (
    search,
    split,
    util,
)
from neptune_fetcher.internal.retrieval.metrics import (
    FloatPointValue,
    fetch_multiple_series_values,
)
from neptune_fetcher.internal.retrieval.search import ContainerType

__all__ = ("fetch_metrics",)

from neptune_fetcher.internal.retrieval.split import split_series_attributes


def fetch_metrics(
    *,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: _Filter,
    attributes: _AttributeFilter,
    include_time: Optional[Literal["absolute"]],
    step_range: tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    context: Optional[Context] = None,
    container_type: ContainerType,
) -> pd.DataFrame:
    _validate_step_range(step_range)
    _validate_tail_limit(tail_limit)
    _validate_include_time(include_time)

    valid_context = validate_context(context or get_context())
    client = get_client(context=valid_context)

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

        metrics_data, sys_id_to_label_mapping = _fetch_metrics(
            filter_=filter_,
            attributes=attributes,
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


def _validate_step_range(step_range: tuple[Optional[float], Optional[float]]) -> None:
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


def _validate_include_time(include_time: Optional[Literal["absolute"]]) -> None:
    if include_time is not None:
        if include_time not in ["absolute"]:
            raise ValueError("include_time must be 'absolute'")


def _fetch_metrics(
    filter_: _Filter,
    attributes: _AttributeFilter,
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
    def fetch_values(
        exp_paths: list[identifiers.RunAttributeDefinition],
    ) -> tuple[list[concurrent.futures.Future], dict[identifiers.RunAttributeDefinition, list[FloatPointValue]]]:
        _series = fetch_multiple_series_values(
            client,
            run_attribute_definitions=exp_paths,
            include_inherited=lineage_to_the_root,
            include_preview=include_point_previews,
            step_range=step_range,
            tail_limit=tail_limit,
        )
        return [], _series

    def process_definitions(
        _sys_ids: Iterable[identifiers.SysId],
        _definitions: Generator[util.Page[identifiers.AttributeDefinition], None, None],
    ) -> tuple[list[concurrent.futures.Future], dict[identifiers.RunAttributeDefinition, list[FloatPointValue]]]:
        definitions_page = next(_definitions, None)
        _futures = []

        if definitions_page:
            _futures.append(executor.submit(process_definitions, _sys_ids, _definitions))

            paths = definitions_page.items

            exp_paths = [
                identifiers.RunAttributeDefinition(RunIdentifier(project_identifier, sys_id), _path)
                for sys_id, _path in it.product(_sys_ids, paths)
                if _path.type == "float_series"
            ]

            for batch in split_series_attributes(items=exp_paths, get_path=lambda r: r.attribute_definition.name):
                _futures.append(executor.submit(fetch_values, batch))

        return _futures, {}

    def process_sys_ids(
        sys_ids_generator: Generator[list[identifiers.SysId], None, None],
    ) -> tuple[list[concurrent.futures.Future], dict[identifiers.RunAttributeDefinition, list[FloatPointValue]]]:
        sys_ids = next(sys_ids_generator, None)
        _futures = []

        if sys_ids:
            _futures.append(executor.submit(process_sys_ids, sys_ids_generator))

            for sys_ids_split in split.split_sys_ids(sys_ids):
                run_identifiers_split = [
                    identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split
                ]
                definitions_generator = fetch_attribute_definitions(
                    client=client,
                    project_identifiers=[project_identifier],
                    run_identifiers=run_identifiers_split,
                    attribute_filter=attributes,
                    executor=fetch_attribute_definitions_executor,
                )
                _futures.append(executor.submit(process_definitions, sys_ids_split, definitions_generator))

        return _futures, {}

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

    futures = {executor.submit(lambda: process_sys_ids(go_fetch_sys_attrs()))}

    metrics_data: dict[identifiers.RunAttributeDefinition, list[FloatPointValue]] = {}

    while futures:
        done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
        futures = not_done
        for future in done:
            new_futures, values = future.result()
            futures.update(new_futures)
            if values:
                for run_attribute, metric_points in values.items():
                    metrics_data.setdefault(run_attribute, []).extend(metric_points)

    return metrics_data, sys_id_label_mapping
