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
from typing import (
    Any,
    Generator,
    Iterable,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from neptune_api.api.retrieval import get_series_values_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import SeriesValuesRequest
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import (
    ProtoPointValueDTO,
    ProtoSeriesValuesResponseDTO,
)
from neptune_api.types import UNSET

from neptune_fetcher.internal.identifiers import RunAttributeDefinition
from neptune_fetcher.internal.retrieval import (
    retry,
    util,
)
from neptune_fetcher.internal.retrieval.attribute_types import (
    File,
    Histogram,
)

SeriesValue = NamedTuple("SeriesValue", [("step", float), ("value", Any), ("timestamp_millis", float)])


def fetch_series_values(
    client: AuthenticatedClient,
    run_attribute_definitions: Iterable[RunAttributeDefinition],
    include_inherited: bool,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,
) -> Generator[util.Page[tuple[RunAttributeDefinition, list[SeriesValue]]], None, None]:
    if not run_attribute_definitions:
        yield from []
        return

    run_attribute_definitions = list(run_attribute_definitions)
    width = len(str(len(run_attribute_definitions) - 1))
    request_id_to_run_attr_definition: dict[str, RunAttributeDefinition] = {
        f"{ix:0{width}d}": pair for ix, pair in enumerate(run_attribute_definitions)
    }

    params: dict[str, Any] = {
        "requests": [
            {
                "requestId": request_id,
                "series": {
                    "holder": {
                        "identifier": f"{run_definition.run_identifier}",
                        "type": "experiment",
                    },
                    "attribute": run_definition.attribute_definition.name,
                    "lineage": "FULL" if include_inherited else "NONE",
                },
            }
            for request_id, run_definition in request_id_to_run_attr_definition.items()
        ],
        "stepRange": {"from": step_range[0], "to": step_range[1]},
        "order": "ascending" if tail_limit is None else "descending",
    }
    if tail_limit is not None:
        params["perSeriesPointsLimit"] = tail_limit

    yield from util.fetch_pages(
        client=client,
        fetch_page=_fetch_series_page,
        process_page=ft.partial(
            _process_series_page, request_id_to_run_attr_definition=request_id_to_run_attr_definition
        ),
        make_new_page_params=_make_new_series_page_params,
        params=params,
    )


def _fetch_series_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> ProtoSeriesValuesResponseDTO:
    body = SeriesValuesRequest.from_dict(params)

    response = retry.handle_errors_default(get_series_values_proto.sync_detailed)(
        client=client, body=body, use_deprecated_string_fields=False
    )

    return ProtoSeriesValuesResponseDTO.FromString(response.content)


def _process_series_page(
    data: ProtoSeriesValuesResponseDTO,
    request_id_to_run_attr_definition: dict[str, RunAttributeDefinition],
) -> util.Page[tuple[RunAttributeDefinition, list[SeriesValue]]]:
    items: dict[RunAttributeDefinition, list[SeriesValue]] = {}

    for series in data.series:
        if series.seriesValues.values:
            run_definition = request_id_to_run_attr_definition[series.requestId]
            values = [_extract_series_value(value) for value in series.seriesValues.values]
            items.setdefault(run_definition, []).extend(values)

    return util.Page(items=list(items.items()))


def _extract_series_value(value_dto: ProtoPointValueDTO) -> SeriesValue:
    obj = value_dto.object
    if obj.HasField("stringValue"):
        value = str(obj.stringValue)
    elif obj.HasField("fileRef"):
        file_ref = obj.fileRef
        value = File(
            path=file_ref.path,
            mime_type=file_ref.mimeType,
            size_bytes=file_ref.sizeBytes,
        )
    elif obj.HasField("histogram"):
        histogram = obj.histogram
        value = Histogram(
            type=str(histogram.type),
            values=list(histogram.values),
            edges=list(histogram.edges),
        )
    else:
        raise ValueError("Series has no valid value")
    return SeriesValue(value_dto.step, value, timestamp_millis=value_dto.timestamp_millis)


def _make_new_series_page_params(
    params: dict[str, Any], data: Optional[ProtoSeriesValuesResponseDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        for request in params["requests"]:
            request.pop("searchAfter", None)
        return params

    # series does not exist: series is missing from the response
    # series finished: series.HasField("searchAfter") == True, series.searchAfter.finished == True,
    #   series.searchAfter.token == 'nonempty'
    # series sent partially: series.HasField("searchAfter") == True, series.searchAfter.finished == False,
    #   series.searchAfter.token == 'nonempty'
    # series exists, but is outside the current page: series.HasField("searchAfter") == False
    # if an attribute does not exist at all, the backend will keep returning an empty searchAfter for it
    # so we stop requesting when there has been no progress, even though some requests may still be unfinished
    existing_series = {series.requestId for series in data.series}
    finished_requests = {
        series.requestId for series in data.series if series.HasField("searchAfter") and series.searchAfter.finished
    }
    updated_request_tokens = {
        series.requestId: {"searchAfter": {"finished": False, "token": series.searchAfter.token}}
        for series in data.series
        if series.HasField("searchAfter")
    }

    if not updated_request_tokens:
        return None

    new_requests = [
        request | updated_request_tokens.get(request["requestId"], {"searchAfter": UNSET})
        for request in params["requests"]
        if request["requestId"] in existing_series and request["requestId"] not in finished_requests
    ]

    params["requests"] = new_requests
    return params
