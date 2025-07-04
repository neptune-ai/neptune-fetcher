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

import functools as ft
import logging
from typing import (
    Any,
    Optional,
    Union,
)

from neptune_api.api.retrieval import get_multiple_float_series_values_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import FloatTimeSeriesValuesRequest
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO

from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.retrieval import (
    retry,
    util,
)

logger = logging.getLogger(__name__)

# Tuples are used here to enhance performance
FloatPointValue = tuple[float, float, float, bool, float]
(
    TimestampIndex,
    StepIndex,
    ValueIndex,
    IsPreviewIndex,
    PreviewCompletionIndex,
) = range(5)

TOTAL_POINT_LIMIT: int = 1_000_000


def fetch_multiple_series_values(
    client: AuthenticatedClient,
    run_attribute_definitions: list[identifiers.RunAttributeDefinition],
    include_inherited: bool,
    include_preview: bool,
    step_range: tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> dict[identifiers.RunAttributeDefinition, list[FloatPointValue]]:
    if not run_attribute_definitions:
        return {}

    assert len(run_attribute_definitions) <= TOTAL_POINT_LIMIT, (
        f"The number of requested attributes {len(run_attribute_definitions)} exceeds the maximum limit of "
        f"{TOTAL_POINT_LIMIT}. Please reduce the number of attributes."
    )

    width = len(str(len(run_attribute_definitions) - 1))
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition] = {
        f"{i:0{width}d}": attr for i, attr in enumerate(run_attribute_definitions)
    }

    params: dict[str, Any] = {
        "requests": [
            {
                "requestId": request_id,
                "series": {
                    "holder": {
                        "identifier": str(run_attribute.run_identifier),
                        "type": "experiment",
                    },
                    "attribute": run_attribute.attribute_definition.name,
                    "lineage": "FULL" if include_inherited else "NONE",
                    "includePreview": include_preview,
                },
            }
            for request_id, run_attribute in request_id_to_attribute.items()
        ],
        "stepRange": {"from": step_range[0], "to": step_range[1]},
        "order": "ascending" if not tail_limit else "descending",
    }

    results: dict[identifiers.RunAttributeDefinition, list[FloatPointValue]] = {
        run_attribute: [] for run_attribute in run_attribute_definitions
    }

    for page_result in util.fetch_pages(
        client=client,
        fetch_page=_fetch_metrics_page,
        process_page=ft.partial(_process_metrics_page, request_id_to_attribute=request_id_to_attribute),
        make_new_page_params=ft.partial(
            _make_new_metrics_page_params,
            request_id_to_attribute=request_id_to_attribute,
            tail_limit=tail_limit,
            partial_results=results,
        ),
        params=params,
    ):
        for attribute, values in page_result.items:
            sorted_values = values if tail_limit else reversed(values)
            results[attribute].extend(sorted_values)

    return results


def _fetch_metrics_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> ProtoFloatSeriesValuesResponseDTO:
    body = FloatTimeSeriesValuesRequest.from_dict(params)

    response = retry.handle_errors_default(get_multiple_float_series_values_proto.sync_detailed)(
        client=client, body=body
    )

    return ProtoFloatSeriesValuesResponseDTO.FromString(response.content)


def _process_metrics_page(
    data: ProtoFloatSeriesValuesResponseDTO,
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition],
) -> util.Page[tuple[identifiers.RunAttributeDefinition, list[FloatPointValue]]]:
    result = {}
    for series in data.series:
        run_attribute = request_id_to_attribute[series.requestId]
        result[run_attribute] = [
            (
                point.timestamp_millis,
                point.step,
                point.value,
                point.is_preview,
                point.completion_ratio,
            )
            for point in series.series.values
        ]
    return util.Page(items=list(result.items()))


def _make_new_metrics_page_params(
    params: dict[str, Any],
    data: Optional[ProtoFloatSeriesValuesResponseDTO],
    request_id_to_attribute: dict[str, identifiers.RunAttributeDefinition],
    tail_limit: Optional[int],
    partial_results: dict[identifiers.RunAttributeDefinition, list[FloatPointValue]],
) -> Optional[dict[str, Any]]:
    if data is None:  # no past data, we are fetching the first page
        for request in params["requests"]:
            if "afterStep" in request:
                del request["afterStep"]
        per_series_points_limit = max(1, TOTAL_POINT_LIMIT // len(params["requests"]))
        if tail_limit is not None:
            per_series_points_limit = min(per_series_points_limit, tail_limit)
        params["perSeriesPointsLimit"] = per_series_points_limit
        return params

    prev_per_series_points_limit = params["perSeriesPointsLimit"]

    new_request_after_steps = {}
    for series in data.series:
        request_id = series.requestId
        value_size = len(series.series.values)
        is_page_full = value_size == prev_per_series_points_limit

        attribute = request_id_to_attribute[request_id]
        need_more_points = len(partial_results[attribute]) < tail_limit if tail_limit is not None else True

        if is_page_full and need_more_points:
            new_request_after_steps[request_id] = series.series.values[-1].step

    if not new_request_after_steps:  # no data left to fetch, return None to stop
        return None

    new_requests = []
    for request in params["requests"]:
        request_id = request["requestId"]
        if request_id in new_request_after_steps:
            after_step = new_request_after_steps[request_id]
            request["afterStep"] = after_step
            new_requests.append(request)
    params["requests"] = new_requests

    per_series_points_limit = max(1, TOTAL_POINT_LIMIT // len(params["requests"]))
    if tail_limit is not None:
        already_fetched = next(
            len(partial_results[request_id_to_attribute[request_id]]) for request_id in new_request_after_steps.keys()
        )  # assumes the results for all unfinished series have the same length
        per_series_points_limit = min(per_series_points_limit, tail_limit - already_fetched)
    params["perSeriesPointsLimit"] = per_series_points_limit

    return params
