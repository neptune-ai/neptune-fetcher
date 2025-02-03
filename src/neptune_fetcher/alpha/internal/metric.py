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
import logging
from concurrent.futures import Executor
from dataclasses import dataclass
from itertools import chain
from typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np
import pandas as pd
from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import get_multiple_float_series_values_proto
from neptune_retrieval_api.models import (
    AttributesHolderIdentifier,
    FloatTimeSeriesValuesRequest,
    FloatTimeSeriesValuesRequestOrder,
    FloatTimeSeriesValuesRequestSeries,
    OpenRangeDTO,
    TimeSeries,
    TimeSeriesLineage,
)
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import (
    identifiers,
    util,
)
from neptune_fetcher.alpha.internal.attribute import (
    AttributeDefinition,
    fetch_attribute_definitions,
)
from neptune_fetcher.alpha.internal.experiment import (
    ExperimentSysAttrs,
    fetch_experiment_sys_attrs,
)
from neptune_fetcher.alpha.internal.identifiers import ExperimentIdentifier as ExpId

logger = logging.getLogger(__name__)

T = TypeVar("T")
AttributePath = str
ExperimentName = str

# Tuples are used here to enhance performance
_FloatPointValue = Tuple[ExperimentName, AttributePath, float, float, float]
ExperimentNameIndex, AttributePathIndex, TimestampIndex, StepIndex, ValueIndex = range(5)

TOTAL_POINT_LIMIT: int = 1_000_000
PATHS_PER_BATCH: int = 10_000


@dataclass(frozen=True)
class _AttributePathInExperiment:
    experiment_identifier: identifiers.ExperimentIdentifier
    experiment_name: str
    attribute_path: AttributePath


def _batch(iterable: List[T], n: int) -> Iterable[List[T]]:
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


def fetch_flat_dataframe_metrics(
    experiments: Filter,
    attributes: AttributeFilter,
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
) -> pd.DataFrame:
    def fetch_values(
        exp_paths: List[_AttributePathInExperiment],
    ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValue]]:
        _series = _fetch_multiple_series_values(
            client,
            exp_paths=exp_paths,
            include_inherited=lineage_to_the_root,
            step_range=step_range,
            tail_limit=tail_limit,
        )
        return [], _series

    def process_definitions(
        _experiments: List[ExperimentSysAttrs],
        _definitions: Generator[util.Page[AttributeDefinition], None, None],
    ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValue]]:
        definitions_page = next(_definitions, None)
        _futures = []
        if definitions_page:
            _futures.append(executor.submit(process_definitions, _experiments, _definitions))

            paths = definitions_page.items

            product = it.product(_experiments, paths)
            exp_paths = [
                _AttributePathInExperiment(ExpId(project, _exp.sys_id), _exp.sys_name, _path.name)
                for _exp, _path in product
                if _path.type == "float_series"
            ]

            for batch in _batch(exp_paths, PATHS_PER_BATCH):
                _futures.append(executor.submit(fetch_values, batch))

        return _futures, []

    def process_experiments(
        experiment_generator: Generator[util.Page[ExperimentSysAttrs], None, None]
    ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValue]]:
        _experiments = next(experiment_generator, None)
        _futures = []

        if _experiments:
            _futures.append(executor.submit(process_experiments, experiment_generator))

        if _experiments and _experiments.items:
            experiment_identifiers = [
                identifiers.ExperimentIdentifier(project, experiment.sys_id) for experiment in _experiments.items
            ]
            sys_id_to_sys_attrs = {exp.sys_id: exp for exp in _experiments.items}

            for experiment_identifiers_split in util.split_experiments(experiment_identifiers):
                sys_attrs_split = [sys_id_to_sys_attrs[exp.sys_id] for exp in experiment_identifiers_split]
                definitions_generator = fetch_attribute_definitions(
                    client=client,
                    project_identifiers=[project],
                    experiment_identifiers=experiment_identifiers_split,
                    attribute_filter=attributes,
                    executor=fetch_attribute_definitions_executor,
                )
                _futures.append(executor.submit(process_definitions, sys_attrs_split, definitions_generator))

        return _futures, []

    def _start() -> Iterable[_FloatPointValue]:
        futures = {
            executor.submit(lambda: process_experiments(fetch_experiment_sys_attrs(client, project, experiments)))
        }

        while futures:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = not_done
            for future in done:
                new_futures, values = future.result()
                futures.update(new_futures)
                if values:
                    yield from values

    df = _create_flat_dataframe(_start())
    return df


def _create_flat_dataframe(values: Iterable[_FloatPointValue]) -> pd.DataFrame:
    """
    Creates a memory-efficient DataFrame directly from _FloatPointValue tuples
    by converting strings to categorical codes before DataFrame creation.
    """

    path_mapping: Dict[str, int] = {}
    experiment_mapping: Dict[str, int] = {}

    def generate_categorized_rows(float_point_values: Iterable[_FloatPointValue]) -> pd.DataFrame:
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
        ("experiment", "uint32"),
        ("path", "uint32"),
        ("timestamp", "uint64"),
        ("step", "float64"),
        ("value", "float64"),
    ]

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(values), dtype=types),
    )
    experiment_dtype = pd.CategoricalDtype(categories=list(experiment_mapping.keys()))
    df["experiment"] = pd.Categorical.from_codes(df["experiment"], dtype=experiment_dtype)

    path_dtype = pd.CategoricalDtype(categories=list(path_mapping.keys()))
    df["path"] = pd.Categorical.from_codes(df["path"], dtype=path_dtype)

    return df


@dataclass(frozen=True)
class _SeriesRequest:
    path: str
    experiment_identifier: identifiers.ExperimentIdentifier
    include_inherited: bool
    after_step: Optional[float]


def _fetch_multiple_series_values(
    client: AuthenticatedClient,
    exp_paths: List[_AttributePathInExperiment],
    include_inherited: bool,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> Iterable[_FloatPointValue]:
    results = []
    partial_results: Dict[_AttributePathInExperiment, List[_FloatPointValue]] = {exp_path: [] for exp_path in exp_paths}
    attribute_steps = {exp_path: None for exp_path in exp_paths}

    while attribute_steps:
        per_series_point_limit = TOTAL_POINT_LIMIT // len(attribute_steps)
        per_series_point_limit = min(per_series_point_limit, tail_limit) if tail_limit else per_series_point_limit

        requests = {
            exp_path: _SeriesRequest(
                path=exp_path.attribute_path,
                experiment_identifier=exp_path.experiment_identifier,
                include_inherited=include_inherited,
                after_step=after_step,
            )
            for exp_path, after_step in attribute_steps.items()
        }

        values = _fetch_series_values(
            client=client,
            requests=requests,
            step_range=step_range,
            per_series_point_limit=per_series_point_limit,
            order="asc" if not tail_limit else "desc",
        )

        new_attribute_steps = {}

        for path, series_values in values.items():
            sorted_list = series_values if not tail_limit else reversed(series_values)
            partial_results[path].extend(sorted_list)

            is_page_full = len(series_values) == per_series_point_limit
            need_more_points = tail_limit is None or len(partial_results[path]) < tail_limit
            if is_page_full and need_more_points:
                new_attribute_steps[path] = series_values[-1][StepIndex]
            else:
                path_result = partial_results.pop(path)
                if path_result:
                    results.append(path_result)
        attribute_steps = new_attribute_steps  # type: ignore

    return chain.from_iterable(results)


def _fetch_series_values(
    client: AuthenticatedClient,
    requests: Dict[_AttributePathInExperiment, _SeriesRequest],
    per_series_point_limit: int,
    step_range: Tuple[Union[float, None], Union[float, None]],
    order: Literal["asc", "desc"],
) -> Dict[_AttributePathInExperiment, List[_FloatPointValue]]:
    series_requests_ids = {}
    series_requests = []

    for ix, (exp_path, request) in enumerate(requests.items()):
        id = f"{ix}"
        series_requests_ids[id] = exp_path
        series_requests.append(
            FloatTimeSeriesValuesRequestSeries(
                request_id=id,
                series=TimeSeries(
                    attribute=exp_path.attribute_path,
                    holder=AttributesHolderIdentifier(
                        identifier=str(exp_path.experiment_identifier),
                        type="experiment",
                    ),
                    lineage=TimeSeriesLineage.FULL if request.include_inherited else TimeSeriesLineage.NONE,
                ),
                after_step=request.after_step,
            )
        )

    response = util.backoff_retry(
        lambda: get_multiple_float_series_values_proto.sync_detailed(
            client=client,
            body=FloatTimeSeriesValuesRequest(
                per_series_points_limit=per_series_point_limit,
                requests=series_requests,
                step_range=OpenRangeDTO(
                    from_=step_range[0],
                    to=step_range[1],
                ),
                order=(
                    FloatTimeSeriesValuesRequestOrder.ASCENDING
                    if order == "asc"
                    else FloatTimeSeriesValuesRequestOrder.DESCENDING
                ),
            ),
        )
    )

    data = ProtoFloatSeriesValuesResponseDTO.FromString(response.content)

    result = {
        series_requests_ids[series.requestId]: [
            (
                series_requests_ids[series.requestId].experiment_name,
                series_requests_ids[series.requestId].attribute_path,
                point.timestamp_millis,
                point.step,
                point.value,
            )
            for point in series.series.values
        ]
        for series in data.series
    }

    return result
