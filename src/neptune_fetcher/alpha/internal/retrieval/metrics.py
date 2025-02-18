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

import logging
from dataclasses import dataclass
from itertools import chain
from typing import (
    Iterable,
    Literal,
    Optional,
    Tuple,
    Union,
)

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

from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.retrieval import util

logger = logging.getLogger(__name__)

AttributePath = str
RunLabel = str

# Tuples are used here to enhance performance
FloatPointValue = Tuple[RunLabel, AttributePath, float, float, float, bool, float]
(
    ExperimentNameIndex,
    AttributePathIndex,
    TimestampIndex,
    StepIndex,
    ValueIndex,
    IsPreviewIndex,
    PreviewCompletionIndex,
) = range(7)

TOTAL_POINT_LIMIT: int = 1_000_000


@dataclass(frozen=True)
class AttributePathInRun:
    run_identifier: identifiers.RunIdentifier
    run_label: RunLabel
    attribute_path: AttributePath


@dataclass(frozen=True)
class _SeriesRequest:
    path: str
    run_identifier: identifiers.RunIdentifier
    include_inherited: bool
    include_preview: bool
    after_step: Optional[float]


def fetch_multiple_series_values(
    client: AuthenticatedClient,
    exp_paths: list[AttributePathInRun],
    include_inherited: bool,
    include_preview: bool,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> Iterable[FloatPointValue]:
    results = []
    partial_results: dict[AttributePathInRun, list[FloatPointValue]] = {exp_path: [] for exp_path in exp_paths}
    attribute_steps = {exp_path: None for exp_path in exp_paths}

    while attribute_steps:
        per_series_point_limit = TOTAL_POINT_LIMIT // len(attribute_steps)
        per_series_point_limit = min(per_series_point_limit, tail_limit) if tail_limit else per_series_point_limit

        requests = {
            exp_path: _SeriesRequest(
                path=exp_path.attribute_path,
                run_identifier=exp_path.run_identifier,
                include_inherited=include_inherited,
                include_preview=include_preview,
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
    requests: dict[AttributePathInRun, _SeriesRequest],
    per_series_point_limit: int,
    step_range: Tuple[Union[float, None], Union[float, None]],
    order: Literal["asc", "desc"],
) -> dict[AttributePathInRun, list[FloatPointValue]]:
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
                        identifier=str(exp_path.run_identifier),
                        type="experiment",
                    ),
                    lineage=TimeSeriesLineage.FULL if request.include_inherited else TimeSeriesLineage.NONE,
                    include_preview=request.include_preview,
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
                series_requests_ids[series.requestId].run_label,
                series_requests_ids[series.requestId].attribute_path,
                point.timestamp_millis,
                point.step,
                point.value,
                point.is_preview,
                point.completion_ratio,
            )
            for point in series.series.values
        ]
        for series in data.series
    }

    return result
