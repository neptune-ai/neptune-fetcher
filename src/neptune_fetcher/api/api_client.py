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

from __future__ import annotations

__all__ = ("ApiClient",)

import re
from dataclasses import dataclass
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from neptune_api.api.backend import get_project
from neptune_api.api.retrieval import (
    get_multiple_float_series_values_proto,
    query_attribute_definitions_proto,
    query_attribute_definitions_within_project,
    query_attributes_within_project_proto,
    search_leaderboard_entries_proto,
)
from neptune_api.credentials import Credentials
from neptune_api.models import (
    AttributesHolderIdentifier,
    FloatTimeSeriesValuesRequest,
    FloatTimeSeriesValuesRequestOrder,
    FloatTimeSeriesValuesRequestSeries,
    OpenRangeDTO,
    ProjectDTO,
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
    QueryAttributesBodyDTO,
    SearchLeaderboardEntriesParamsDTO,
    TimeSeries,
    TimeSeriesLineage,
)
from neptune_api.proto.neptune_pb.api.v1.model.attributes_pb2 import (
    ProtoAttributesSearchResultDTO,
    ProtoQueryAttributesResultDTO,
)
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import ProtoLeaderboardEntriesSearchResultDTO
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO

from neptune_fetcher.fields import (
    FieldDefinition,
    FieldType,
    FloatPointValue,
)
from neptune_fetcher.internal.api_utils import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune_fetcher.internal.retrieval.retry import handle_errors_default
from neptune_fetcher.util import (
    NeptuneException,
    rethrow_neptune_error,
)


class ApiClient:
    def __init__(self, api_token: str, proxies: Optional[Dict[str, str]] = None) -> None:
        credentials = Credentials.from_api_key(api_key=api_token)
        config, token_urls = rethrow_neptune_error(get_config_and_token_urls)(credentials=credentials, proxies=proxies)
        self._backend = create_auth_api_client(
            credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=proxies
        )

    def cleanup(self) -> None:
        pass

    def close(self) -> None:
        self._backend.__exit__()

    def fetch_series_values(
        self,
        path: str,
        include_inherited: bool,
        container_id: str,
        include_point_previews: bool,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> Iterator[FloatPointValue]:
        step_size: int = 10_000

        current_batch_size = None
        last_step_value = None
        while current_batch_size is None or current_batch_size == step_size:
            request = _SeriesRequest(
                path=path,
                container_id=container_id,
                include_inherited=include_inherited,
                after_step=last_step_value,
                include_point_previews=include_point_previews,
            )

            batch = self._fetch_series_values(
                requests=[request],
                step_range=step_range,
                limit=step_size,
            )[0]

            yield from batch

            current_batch_size = len(batch)
            last_step_value = batch[-1].step if batch else None

    def fetch_multiple_series_values(
        self,
        paths: list[str],
        include_inherited: bool,
        container_id: str,
        include_point_previews: bool = True,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> Iterator[Tuple[str, List[FloatPointValue]]]:
        total_step_limit: int = 1_000_000

        paths_len = len(paths)
        if paths_len > total_step_limit:
            raise ValueError(f"The number of paths ({paths_len}) exceeds the step limit ({total_step_limit})")

        results = {path: [] for path in paths}
        attribute_steps = {path: None for path in paths}

        while attribute_steps:
            series_step_limit = total_step_limit // len(attribute_steps)
            requests = [
                _SeriesRequest(
                    path=path,
                    container_id=container_id,
                    include_inherited=include_inherited,
                    after_step=after_step,
                    include_point_previews=include_point_previews,
                )
                for path, after_step in attribute_steps.items()
            ]

            values = self._fetch_series_values(
                requests=requests,
                step_range=step_range,
                limit=series_step_limit,
            )

            new_attribute_steps = {}
            for request, series_values in zip(requests, values):
                path = request.path
                results[path].extend(series_values)
                if len(series_values) == series_step_limit:
                    new_attribute_steps[path] = series_values[-1].step
                else:
                    path_result = results.pop(path)
                    yield path, path_result
            attribute_steps = new_attribute_steps

    def _fetch_series_values(
        self,
        requests: List[_SeriesRequest],
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
        limit: int = 10_000,
    ) -> List[List[FloatPointValue]]:
        request = FloatTimeSeriesValuesRequest(
            per_series_points_limit=limit,
            requests=[
                FloatTimeSeriesValuesRequestSeries(
                    request_id=f"{ix}",
                    series=TimeSeries(
                        attribute=request.path,
                        holder=AttributesHolderIdentifier(
                            identifier=request.container_id,
                            type="experiment",
                        ),
                        lineage=TimeSeriesLineage.FULL if request.include_inherited else TimeSeriesLineage.NONE,
                        include_preview=request.include_point_previews,
                    ),
                    after_step=request.after_step,
                )
                for ix, request in enumerate(requests)
            ],
            step_range=OpenRangeDTO(
                from_=step_range[0],
                to=step_range[1],
            ),
            order=FloatTimeSeriesValuesRequestOrder.ASCENDING,
        )

        response = rethrow_neptune_error(handle_errors_default(get_multiple_float_series_values_proto.sync_detailed))(
            client=self._backend, body=request
        )

        data: ProtoFloatSeriesValuesResponseDTO = ProtoFloatSeriesValuesResponseDTO.FromString(response.content)

        return [
            [
                FloatPointValue(
                    timestamp=datetime.fromtimestamp(point.timestamp_millis / 1000.0, tz=timezone.utc),
                    value=point.value,
                    step=point.step,
                    preview=point.is_preview,
                    completion_ratio=point.completion_ratio,
                )
                for point in series.series.values
            ]
            for series in sorted(data.series, key=lambda s: int(s.requestId))
        ]

    def query_attribute_definitions(self, container_id: str) -> List[FieldDefinition]:
        response = rethrow_neptune_error(handle_errors_default(query_attribute_definitions_proto.sync_detailed))(
            client=self._backend,
            experiment_identifier=container_id,
        )
        definitions: ProtoAttributesSearchResultDTO = ProtoAttributesSearchResultDTO.FromString(response.content)
        return [
            FieldDefinition(field_definition.name, FieldType(field_definition.type))
            for field_definition in definitions.entries
        ]

    def query_attributes_within_project(
        self, project_id: str, body: QueryAttributesBodyDTO
    ) -> ProtoQueryAttributesResultDTO:
        response = rethrow_neptune_error(handle_errors_default(query_attributes_within_project_proto.sync_detailed))(
            client=self._backend, body=body, project_identifier=project_id
        )
        data: ProtoQueryAttributesResultDTO = ProtoQueryAttributesResultDTO.FromString(response.content)
        return data

    def find_field_type_within_project(self, project_id: str, attribute_name: str) -> Set[str]:
        sortable_attributes = [FieldType.STRING, FieldType.FLOAT, FieldType.INT, FieldType.BOOL, FieldType.DATETIME]
        body = QueryAttributeDefinitionsBodyDTO.from_dict(
            {
                "attributeNameFilter": {"mustMatchRegexes": [f"^{re.escape(attribute_name)}$"]},
                "nextPage": {"limit": 1000},
                "attributeFilter": [{"attributeType": t.value} for t in sortable_attributes],
                "projectIdentifiers": [project_id],
            }
        )

        response = rethrow_neptune_error(
            handle_errors_default(query_attribute_definitions_within_project.sync_detailed)
        )(client=self._backend, body=body)

        data: QueryAttributeDefinitionsResultDTO = response.parsed

        return {t.type.value for t in data.entries}

    def search_entries(
        self, project_id: str, body: SearchLeaderboardEntriesParamsDTO
    ) -> ProtoLeaderboardEntriesSearchResultDTO:
        resp = rethrow_neptune_error(handle_errors_default(search_leaderboard_entries_proto.sync_detailed))(
            client=self._backend, project_identifier=project_id, type=["run"], body=body
        )
        proto_data = ProtoLeaderboardEntriesSearchResultDTO.FromString(resp.content)
        return proto_data

    def project_name_lookup(self, name: Optional[str] = None) -> ProjectDTO:
        response = rethrow_neptune_error(handle_errors_default(get_project.sync_detailed))(
            client=self._backend, project_identifier=name
        )
        if response.status_code.value == 404:
            raise NeptuneException("Project not found")
        else:
            return response.parsed


@dataclass(frozen=True)
class _SeriesRequest:
    path: str
    container_id: str
    include_inherited: bool
    after_step: Optional[float]
    include_point_previews: bool = False
