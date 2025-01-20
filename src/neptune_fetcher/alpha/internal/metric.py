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
import logging
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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

logger = logging.getLogger(__name__)

T = TypeVar("T")

_FloatPointValue = namedtuple("_FloatPointValue", ["experiment", "path", "timestamp", "step", "value"])


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
        start = time.time()

        def fetch_values(
            _experiment: ExperimentSysAttrs, _definitions: List[str]
        ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValue]]:
            _series = _fetch_multiple_series_values(
                client,
                _definitions,
                include_inherited=lineage_to_the_root,
                container_id=identifiers.ExperimentIdentifier(project, _experiment.sys_id),
                experiment=_experiment.sys_name,
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

                # Filter out only float series
                paths = [definition.name for definition in definitions_page.items if definition.type == "float_series"]
                if paths:
                    for experiment in _experiments:
                        _futures.append(executor.submit(fetch_values, experiment, paths))

            return _futures, []

        def process_experiments(
            experiment_generator: Generator[util.Page[ExperimentSysAttrs], None, None]
        ) -> Tuple[List[concurrent.futures.Future], Iterable[_FloatPointValue]]:
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

        series: List[Iterable[_FloatPointValue]] = []
        while futures:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = not_done
            for future in done:
                new_futures, values = future.result()
                futures.update(new_futures)
                series.append(values)

    end = time.time()
    df = pd.DataFrame(chain.from_iterable(series))
    logger.debug(f"Time taken: {end - start}s Number of points: {len(df)}")
    return df


@dataclass(frozen=True)
class _SeriesRequest:
    path: str
    container_id: str
    include_inherited: bool
    after_step: Optional[float]


def _fetch_multiple_series_values(
    client: AuthenticatedClient,
    paths: List[str],
    include_inherited: bool,
    container_id: identifiers.ExperimentIdentifier,
    experiment: str,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> Iterable[_FloatPointValue]:
    total_step_limit: int = 1_000_000

    paths_len = len(paths)
    if paths_len > total_step_limit:
        raise ValueError(f"The number of paths ({paths_len}) exceeds the step limit ({total_step_limit})")

    results: Dict[str, List[_FloatPointValue]] = {path: [] for path in paths}
    attribute_steps = {path: None for path in paths}

    fetched_series = []
    while attribute_steps:
        series_step_limit = total_step_limit // len(attribute_steps)
        series_step_limit = min(series_step_limit, tail_limit) if tail_limit else series_step_limit

        requests = {
            path: _SeriesRequest(
                path=path,
                container_id=str(container_id),
                include_inherited=include_inherited,
                after_step=after_step,
            )
            for path, after_step in attribute_steps.items()
        }

        values = _fetch_series_values(
            client=client,
            requests=requests,
            experiment=experiment,
            step_range=step_range,
            limit=series_step_limit,
            order="desc" if tail_limit else "asc",
        )

        new_attribute_steps = {}

        for path, series_values in values.items():
            results[path].extend(series_values)
            if len(series_values) == series_step_limit and (tail_limit is None or len(results[path]) < tail_limit):
                new_attribute_steps[path] = series_values[-1].step
            else:
                path_result = results.pop(path)
                if path_result:
                    fetched_series.append(path_result)
        attribute_steps = new_attribute_steps

    return chain.from_iterable(fetched_series)


def _fetch_series_values(
    client: AuthenticatedClient,
    requests: Dict[str, _SeriesRequest],
    experiment: str,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    limit: int = 10_000,
    order: Literal["asc", "desc"] = "asc",
) -> Dict[str, List[_FloatPointValue]]:
    series_requests_ids = {}
    series_requests = []

    for ix, (path, request) in enumerate(requests.items()):
        id = f"{ix}"
        series_requests_ids[id] = path
        series_requests.append(
            FloatTimeSeriesValuesRequestSeries(
                request_id=id,
                series=TimeSeries(
                    attribute=path,
                    holder=AttributesHolderIdentifier(
                        identifier=request.container_id,
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
                per_series_points_limit=limit,
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

    data: ProtoFloatSeriesValuesResponseDTO = ProtoFloatSeriesValuesResponseDTO.FromString(response.content)

    result = {
        series_requests_ids[series.requestId]: [
            _FloatPointValue(
                experiment=experiment,
                path=series_requests_ids[series.requestId],
                timestamp=datetime.fromtimestamp(point.timestamp_millis / 1000.0, tz=timezone.utc),
                value=point.value,
                step=point.step,
            )
            for point in series.series.values
        ]
        for series in data.series
    }

    return result
