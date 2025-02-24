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
    Tuple,
    Union,
)

import pandas as pd
from neptune_api.client import AuthenticatedClient

from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.client import get_client
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.composition.attributes import fetch_attribute_definitions
from neptune_fetcher.alpha.internal.composition.util import batched
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.identifiers import RunIdentifier as ExpId
from neptune_fetcher.alpha.internal.retrieval import (
    split,
    util,
)
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.metrics import (
    AttributePathInRun,
    FloatPointValue,
    fetch_multiple_series_values,
)
from neptune_fetcher.alpha.internal.retrieval.search import (
    ContainerType,
    fetch_experiment_sys_attrs,
    fetch_run_sys_attrs,
)

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
    ...
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

        values_generator = _fetch_flat_dataframe_metrics(
            filter_=filter_,
            attributes=attributes,
            client=client,
            project=project_identifier,
            step_range=step_range,
            lineage_to_the_root=lineage_to_the_root,
            tail_limit=tail_limit,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=container_type,
        )

        index_column_name = "experiment" if container_type == ContainerType.EXPERIMENT else "run"

        df = _create_flat_dataframe(
            values_generator,
            index_column_name=index_column_name,
        )

    return df


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


def _fetch_flat_dataframe_metrics(
    filter_: Filter,
    attributes: AttributeFilter,
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    step_range: Tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    container_type: ContainerType,
) -> Iterable[FloatPointValue]:
    def fetch_values(
        exp_paths: list[AttributePathInRun],
    ) -> Tuple[list[concurrent.futures.Future], Iterable[FloatPointValue]]:
        _series = fetch_multiple_series_values(
            client,
            exp_paths=exp_paths,
            include_inherited=lineage_to_the_root,
            step_range=step_range,
            tail_limit=tail_limit,
        )
        return [], _series

    def process_definitions(
        _sys_id_to_labels: dict[identifiers.SysId, str],
        _definitions: Generator[util.Page[AttributeDefinition], None, None],
    ) -> Tuple[list[concurrent.futures.Future], Iterable[FloatPointValue]]:
        definitions_page = next(_definitions, None)
        _futures = []
        if definitions_page:
            _futures.append(executor.submit(process_definitions, _sys_id_to_labels, _definitions))

            paths = definitions_page.items

            product = it.product(_sys_id_to_labels.items(), paths)
            exp_paths = [
                AttributePathInRun(ExpId(project, sys_id), label, _path.name)
                for (sys_id, label), _path in product
                if _path.type == "float_series"
            ]

            for batch in batched(exp_paths, _PATHS_PER_BATCH):
                _futures.append(executor.submit(fetch_values, batch))

        return _futures, []

    def process_sys_ids(
        sys_ids_generator: Generator[dict[identifiers.SysId, str], None, None]
    ) -> Tuple[list[concurrent.futures.Future], Iterable[FloatPointValue]]:
        sys_id_to_labels = next(sys_ids_generator, None)
        _futures = []

        if sys_id_to_labels:
            _futures.append(executor.submit(process_sys_ids, sys_ids_generator))

            sys_ids = list(sys_id_to_labels.keys())

            for sys_ids_split in split.split_sys_ids(sys_ids):
                run_identifiers_split = [identifiers.RunIdentifier(project, sys_id) for sys_id in sys_ids_split]
                sys_id_to_labels_split = {sys_id: sys_id_to_labels[sys_id] for sys_id in sys_ids_split}
                definitions_generator = fetch_attribute_definitions(
                    client=client,
                    project_identifiers=[project],
                    run_identifiers=run_identifiers_split,
                    attribute_filter=attributes,
                    executor=fetch_attribute_definitions_executor,
                )
                _futures.append(executor.submit(process_definitions, sys_id_to_labels_split, definitions_generator))

        return _futures, []

    def fetch_sys_ids_labels() -> Generator[dict[identifiers.SysId, str], None, None]:
        if container_type == ContainerType.RUN:
            run_pages = fetch_run_sys_attrs(client, project, filter_)
            return ({run.sys_id: run.sys_custom_run_id for run in page.items} for page in run_pages)
        elif container_type == ContainerType.EXPERIMENT:
            experiment_pages = fetch_experiment_sys_attrs(client, project, filter_)
            return ({exp.sys_id: exp.sys_name for exp in page.items} for page in experiment_pages)
        else:
            raise RuntimeError(f"Unknown container type: {container_type}")

    def _start() -> Iterable[FloatPointValue]:
        futures = {executor.submit(lambda: process_sys_ids(fetch_sys_ids_labels()))}

        while futures:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = not_done
            for future in done:
                new_futures, values = future.result()
                futures.update(new_futures)
                if values:
                    yield from values

    return _start()


def _create_flat_dataframe(values: Iterable[FloatPointValue], index_column_name: str) -> pd.DataFrame:
    return pd.DataFrame()
