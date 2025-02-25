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
from dataclasses import dataclass
from typing import (
    Any,
    Generator,
    Iterable,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import get_series_values_proto
from neptune_retrieval_api.models import SeriesValuesRequest
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoSeriesValuesResponseDTO

from neptune_fetcher.alpha.internal.identifiers import RunIdentifier
from neptune_fetcher.alpha.internal.retrieval import util
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition


@dataclass(frozen=True)
class RunAttributeDefinition:
    run_identifier: RunIdentifier
    attribute_definition: AttributeDefinition


StringSeriesValue = NamedTuple("StringSeriesValue", [("step", float), ("value", str), ("timestamp_millis", int)])


def fetch_series_values(
    client: AuthenticatedClient,
    run_attribute_definitions: Iterable[RunAttributeDefinition],
    include_inherited: bool,
    step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    tail_limit: Optional[int] = None,  # TODO: actually implement tail_limit
) -> Generator[util.Page[tuple[RunAttributeDefinition, list[StringSeriesValue]]], None, None]:
    if not run_attribute_definitions:
        yield from []
        return

    request_id_to_runs_definitions: dict[str, RunAttributeDefinition] = {
        str(ix): pair for ix, pair in enumerate(run_attribute_definitions)
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
            for request_id, run_definition in request_id_to_runs_definitions.items()
        ],
        "stepRange": {"from": step_range[0], "to": step_range[1]},
        "order": "ascending" if tail_limit is None else "descending",
        "nextPage": {},
    }

    yield from util.fetch_pages(
        client=client,
        fetch_page=_fetch_series_page,
        process_page=ft.partial(_process_series_page, request_id_to_runs_definitions=request_id_to_runs_definitions),
        make_new_page_params=_make_new_series_page_params,
        params=params,
    )


def _fetch_series_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> ProtoSeriesValuesResponseDTO:
    body = SeriesValuesRequest.from_dict(params)
    response = util.backoff_retry(
        get_series_values_proto.sync_detailed,
        client=client,
        body=body,
    )
    return ProtoSeriesValuesResponseDTO.FromString(response.content)


def _process_series_page(
    data: ProtoSeriesValuesResponseDTO,
    request_id_to_runs_definitions: dict[str, RunAttributeDefinition],
) -> util.Page[tuple[RunAttributeDefinition, list[StringSeriesValue]]]:
    items: dict[RunAttributeDefinition, list[StringSeriesValue]] = {}

    for series in data.series:
        run_definition = request_id_to_runs_definitions[series.requestId]
        values = [
            StringSeriesValue(value.step, value.value, value.timestamp_millis) for value in series.string_series.values
        ]
        items.setdefault(run_definition, []).extend(values)

    return util.Page(items=list(items.items()))


def _make_new_series_page_params(
    params: dict[str, Any], data: Optional[ProtoSeriesValuesResponseDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.nextPageToken
    if not next_page_token:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params
