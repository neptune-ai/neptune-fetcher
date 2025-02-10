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

import numpy as np
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
    AttributePathIndex,
    AttributePathInRun,
    ExperimentNameIndex,
    FloatPointValue,
    StepIndex,
    TimestampIndex,
    ValueIndex,
    fetch_multiple_series_values,
)
from neptune_fetcher.alpha.internal.retrieval.search import (
    ContainerType,
    fetch_experiment_sys_attrs,
    fetch_run_sys_attrs,
)

__all__ = (
    "fetch_experiment_metrics",
    "fetch_run_metrics",
)


_PATHS_PER_BATCH: int = 10_000


def fetch_experiment_metrics(
    experiments: Union[str, Filter],
    attributes: Union[str, AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation).

    `experiments` - a filter specifying which experiments to include
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
                If `AttributeFilter.aggregations` is set, an exception will be raised as
                they're not supported in this function.
    `include_time` - whether to include absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete experiment history.
        If False, only includes points from the most recent experiment in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=attributes, type_in=["float_series"])

    return _fetch_metrics(
        filter_=experiments,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
        container_type=ContainerType.EXPERIMENT,
    )


def fetch_run_metrics(
    runs: Union[str, Filter],
    attributes: Union[str, AttributeFilter],
    include_time: Optional[Literal["absolute"]] = None,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation).

    `runs` - a filter specifying which runs to include
        - a regex that run id must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
                If `AttributeFilter.aggregations` is set, an exception will be raised as
                they're not supported in this function.
    `include_time` - whether to include absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - if True (default), includes all points from the complete run history.
        If False, only includes points from the most recent run in the lineage.
    `tail_limit` - from the tail end of each series, how many points to include at most.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.

    If `include_time` is set, each metric column has an additional sub-column with requested timestamp values.
    """
    if isinstance(runs, str):
        runs = Filter.matches_all(Attribute("sys/custom_run_id", type="string"), regex=runs)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=attributes, type_in=["float_series"])

    return _fetch_metrics(
        filter_=runs,
        attributes=attributes,
        include_time=include_time,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
        tail_limit=tail_limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        context=context,
        container_type=ContainerType.RUN,
    )


def _fetch_metrics(
    filter_: Filter,
    attributes: AttributeFilter,
    include_time: Optional[Literal["absolute"]],
    step_range: Tuple[Optional[float], Optional[float]],
    lineage_to_the_root: bool,
    tail_limit: Optional[int],
    type_suffix_in_column_names: bool,
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

    if include_time == "absolute":
        df = _transform_with_absolute_timestamp(df, type_suffix_in_column_names, index_column_name)
    # elif include_time == "relative":
    #     raise NotImplementedError("Relative timestamp is not implemented")
    else:
        df = _transform_without_timestamp(df, type_suffix_in_column_names, index_column_name)

    return df


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


def _validate_include_time(include_time: Optional[Literal["absolute"]]) -> None:
    if include_time is not None:
        if include_time not in ["absolute"]:
            raise ValueError("include_time must be 'absolute'")


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
) -> Iterable[FloatPointValue]:  # pd.DataFrame:
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


def _create_flat_dataframe(
    values: Iterable[FloatPointValue],
    index_column_name: str = "experiment",
) -> pd.DataFrame:
    """
    Creates a memory-efficient DataFrame directly from _FloatPointValue tuples
    by converting strings to categorical codes before DataFrame creation.
    """

    path_mapping: dict[str, int] = {}
    experiment_mapping: dict[str, int] = {}

    def generate_categorized_rows(float_point_values: Iterable[FloatPointValue]) -> pd.DataFrame:
        last_experiment_name, last_experiment_category = None, None
        last_path_name, last_path_category = None, None

        for point in float_point_values:
            exp_category = (
                last_experiment_category
                if last_experiment_name == point[ExperimentNameIndex]
                else experiment_mapping.get(point[ExperimentNameIndex], None)  # type: ignore
            )
            path_category = (
                last_path_category
                if last_path_name == point[AttributePathIndex]
                else path_mapping.get(point[AttributePathIndex], None)  # type: ignore
            )

            if exp_category is None:
                exp_category = len(experiment_mapping)
                experiment_mapping[point[ExperimentNameIndex]] = exp_category  # type: ignore
                last_experiment_name, last_experiment_category = point[ExperimentNameIndex], exp_category
            if path_category is None:
                path_category = len(path_mapping)
                path_mapping[point[AttributePathIndex]] = path_category  # type: ignore
                last_path_name, last_path_category = point[AttributePathIndex], path_category
            yield exp_category, path_category, point[TimestampIndex], point[StepIndex], point[ValueIndex]

    types = [
        (index_column_name, "uint32"),
        ("path", "uint32"),
        ("timestamp", "uint64"),
        ("step", "float64"),
        ("value", "float64"),
    ]

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(values), dtype=types),
    )
    experiment_dtype = pd.CategoricalDtype(categories=list(experiment_mapping.keys()))
    df[index_column_name] = pd.Categorical.from_codes(df[index_column_name], dtype=experiment_dtype)

    path_dtype = pd.CategoricalDtype(categories=list(path_mapping.keys()))
    df["path"] = pd.Categorical.from_codes(df["path"], dtype=path_dtype)

    return df


def _transform_with_absolute_timestamp(
    df: pd.DataFrame, type_suffix_in_column_names: bool, index_column_name: str = "experiment"
) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", origin="unix", utc=True)
    df = df.rename(columns={"timestamp": "absolute_time"})
    df = df.pivot(
        index=[index_column_name, "step"],
        columns="path",
        values=["value", "absolute_time"],
    )

    df = df.swaplevel(axis=1)
    if type_suffix_in_column_names:
        df = df.rename(columns=lambda x: x + ":float_series", level=0, copy=False)

    df = df.reset_index()
    df[index_column_name] = df[index_column_name].astype(str)
    df = df.sort_values(by=[index_column_name, "step"], ignore_index=True)
    df.columns.names = (None, None)
    df = df.set_index([index_column_name, "step"])
    df = df.sort_index(axis=1, level=0)
    return df


def _transform_without_timestamp(
    df: pd.DataFrame, type_suffix_in_column_names: bool, index_column_name: str = "experiment"
) -> pd.DataFrame:
    df = df.pivot(index=[index_column_name, "step"], columns="path", values="value")
    if type_suffix_in_column_names:
        df = df.rename(columns=lambda x: x + ":float_series", copy=False)

    df = df.reset_index()
    df[index_column_name] = df[index_column_name].astype(str)
    df = df.sort_values(by=[index_column_name, "step"], ignore_index=True)
    df.columns.name = None
    df = df.set_index([index_column_name, "step"])
    df = df.sort_index(axis=1)
    return df
