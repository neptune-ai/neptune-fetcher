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
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)
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

from neptune_fetcher.alpha.filter import (
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import (
    env,
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


@dataclass
class _FloatPointValues:
    experiment: str
    path: str
    steps: List[float] = field(default_factory=lambda: [])
    values: List[float] = field(default_factory=lambda: [])
    timestamps: List[datetime] = field(default_factory=lambda: [])

    def reversed(self) -> "_FloatPointValues":
        return _FloatPointValues(
            experiment=self.experiment,
            path=self.path,
            steps=list(reversed(self.steps)),
            values=list(reversed(self.values)),
            timestamps=list(reversed(self.timestamps)),
        )

    def extend(self, values: "_FloatPointValues") -> None:
        self.steps.extend(values.steps)
        self.values.extend(values.values)
        self.timestamps.extend(values.timestamps)

    def __len__(self) -> int:
        return len(self.steps)


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
        yield iterable[ndx: min(ndx + n, length)]


def _create_series(
    data: Iterable[Iterable[T]],
) -> pd.Series:
    return pd.Series(chain.from_iterable(data))


def _create_category_series(data: Iterable[Iterable[T]], categories: list[str]) -> pd.Categorical:
    dtype = pd.CategoricalDtype(categories=categories)
    return pd.Categorical.from_codes(list(chain.from_iterable(data)), dtype=dtype)


class _ResultAccumulator:
    def __init__(self):  # type: ignore
        self.timestamp_vectors = []
        self.step_vectors = []
        self.value_vectors = []
        self.experiment_name_to_id = OrderedDict()
        self.path_name_to_id = OrderedDict()
        self.experiment_vectors = []
        self.path_vectors = []
        self.next_experiment_id = 0
        self.next_path_id = 0

    def add_float_point_values(self, float_point_values: _FloatPointValues) -> None:
        self.timestamp_vectors.append(float_point_values.timestamps)
        self.step_vectors.append(float_point_values.steps)
        self.value_vectors.append(float_point_values.values)

        if float_point_values.experiment not in self.experiment_name_to_id:
            self.experiment_name_to_id[float_point_values.experiment] = self.next_experiment_id
            self.next_experiment_id += 1

        if float_point_values.path not in self.path_name_to_id:
            self.path_name_to_id[float_point_values.path] = self.next_path_id
            self.next_path_id += 1

        self.experiment_vectors.append(
            it.repeat(self.experiment_name_to_id[float_point_values.experiment], len(float_point_values))
        )
        self.path_vectors.append(it.repeat(self.path_name_to_id[float_point_values.path], len(float_point_values)))

    def create_dataframe(self, executor: ThreadPoolExecutor) -> pd.DataFrame:
        series_futures = {
            "experiment": executor.submit(
                _create_category_series, self.experiment_vectors, list(self.experiment_name_to_id.keys())
            ),
            "path": executor.submit(_create_category_series, self.path_vectors, list(self.path_name_to_id.keys())),
            "timestamp": executor.submit(_create_series, self.timestamp_vectors),
            "step": executor.submit(_create_series, self.step_vectors),
            "value": executor.submit(_create_series, self.value_vectors),
        }
        return pd.DataFrame({key: future.result() for key, future in series_futures.items()}, copy=False)


def fetch_flat_dataframe_metrics(
    experiments: ExperimentFilter,
    attributes: AttributeFilter,
    client: AuthenticatedClient,
    project: identifiers.ProjectIdentifier,
    step_range: Tuple[Optional[float], Optional[float]] = (None, None),
    lineage_to_the_root: bool = True,
    tail_limit: Optional[int] = None,
) -> pd.DataFrame:
    max_workers = env.NEPTUNE_FETCHER_MAX_WORKERS.get()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        def fetch_values(
            exp_paths: List[_AttributePathInExperiment],
        ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValues]]:
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
        ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValues]]:
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
        ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValues]]:
            _experiments = next(experiment_generator, None)
            _futures = []
            if _experiments:
                _futures.append(executor.submit(process_experiments, experiment_generator))
            if _experiments and _experiments.items:
                definitions_generator = fetch_attribute_definitions(
                    client=client,
                    project_identifiers=[project],
                    experiment_identifiers=[
                        identifiers.ExperimentIdentifier(project, experiment.sys_id)
                        for experiment in _experiments.items
                    ],
                    attribute_filter=attributes,
                )
                _futures.append(executor.submit(process_definitions, _experiments.items, definitions_generator))
            return _futures, []

        futures = {
            executor.submit(lambda: process_experiments(fetch_experiment_sys_attrs(client, project, experiments)))
        }

        result_accumulator = _ResultAccumulator()

        while futures:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = not_done
            for future in done:
                new_futures, values = future.result()
                futures.update(new_futures)
                for float_point_values in values:
                    result_accumulator.add_float_point_values(float_point_values)

        dataframe = result_accumulator.create_dataframe(executor)

        return dataframe


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
) -> Iterable[_FloatPointValues]:
    results = []
    partial_results: Dict[_AttributePathInExperiment, _FloatPointValues] = {
        exp_path: _FloatPointValues(experiment=exp_path.experiment_name, path=exp_path.attribute_path)
        for exp_path in exp_paths
    }
    attribute_steps: dict[_AttributePathInExperiment, Optional[float]] = {exp_path: None for exp_path in exp_paths}

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

        new_attribute_steps: dict[_AttributePathInExperiment, Optional[float]] = {}

        for path, series_values in values.items():
            sorted_values = series_values if not tail_limit else series_values.reversed()
            partial_results[path].extend(sorted_values)

            is_page_full = len(series_values) == per_series_point_limit
            need_more_points = tail_limit is None or len(partial_results[path]) < tail_limit
            if is_page_full and need_more_points:
                new_attribute_steps[path] = series_values.steps[-1]
            else:
                path_result = partial_results.pop(path)
                if path_result:
                    results.append(path_result)
        attribute_steps = new_attribute_steps

    return results


def _fetch_series_values(
    client: AuthenticatedClient,
    requests: Dict[_AttributePathInExperiment, _SeriesRequest],
    per_series_point_limit: int,
    step_range: Tuple[Union[float, None], Union[float, None]],
    order: Literal["asc", "desc"],
) -> Dict[_AttributePathInExperiment, _FloatPointValues]:
    series_requests_ids = {}
    series_requests = []

    for ix, (exp_path, request) in enumerate(requests.items()):
        _id = f"{ix}"
        series_requests_ids[_id] = exp_path
        series_requests.append(
            FloatTimeSeriesValuesRequestSeries(
                request_id=_id,
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

    result = {}
    for series in data.series:
        steps = []
        values = []
        timestamps = []
        for point in series.series.values:
            steps.append(point.step)
            values.append(point.value)
            timestamps.append(datetime.fromtimestamp(point.timestamp_millis / 1000.0, tz=timezone.utc))

        result[series_requests_ids[series.requestId]] = _FloatPointValues(
            experiment=series_requests_ids[series.requestId].experiment_name,
            path=series_requests_ids[series.requestId].attribute_path,
            steps=steps,
            values=values,
            timestamps=timestamps,
        )

    return result
