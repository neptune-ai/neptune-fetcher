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
    Literal,
    Optional,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import search_leaderboard_entries_proto
from neptune_retrieval_api.models import SearchLeaderboardEntriesParamsDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)

from neptune_fetcher.alpha.filters import (
    Attribute,
    Filter,
)
from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
    util,
)
from neptune_fetcher.alpha.internal.types import map_attribute_type_python_to_backend

_DIRECTION_PYTHON_TO_BACKEND_MAP: dict[str, str] = {
    "asc": "ascending",
    "desc": "descending",
}


def _map_direction(direction: Literal["asc", "desc"]) -> str:
    return _DIRECTION_PYTHON_TO_BACKEND_MAP[direction]


@dataclass(frozen=True)
class ExperimentSysAttrs:
    sys_name: identifiers.SysName
    sys_id: identifiers.SysId


def fetch_experiment_sys_attrs(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    experiment_filter: Optional[Filter] = None,
    sort_by: Attribute = Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    batch_size: int = env.NEPTUNE_FETCHER_EXPERIMENT_SYS_ATTRS_BATCH_SIZE.get(),
) -> Generator[util.Page[ExperimentSysAttrs], None, None]:
    params: dict[str, Any] = {
        "attributeFilters": [{"path": "sys/name"}, {"path": "sys/id"}],
        "pagination": {"limit": batch_size},
        "experimentLeader": True,
        "sorting": {
            "dir": _map_direction(sort_direction),
            "sortBy": {"name": sort_by.name},
        },
    }
    if experiment_filter is not None:
        params["query"] = {"query": str(experiment_filter)}
    if sort_by.aggregation is not None:
        params["sorting"]["aggregationMode"] = sort_by.aggregation
    if sort_by.type is not None:
        params["sorting"]["sortBy"]["type"] = map_attribute_type_python_to_backend(sort_by.type)

    return util.fetch_pages(
        client=client,
        fetch_page=ft.partial(_fetch_experiment_page, project_identifier=project_identifier),
        process_page=_process_experiment_page,
        make_new_page_params=ft.partial(_make_new_experiment_page_params, batch_size=batch_size, limit=limit),
        params=params,
    )


def _fetch_experiment_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project_identifier: identifiers.ProjectIdentifier,
) -> ProtoLeaderboardEntriesSearchResultDTO:
    body = SearchLeaderboardEntriesParamsDTO.from_dict(params)

    response = util.backoff_retry(
        search_leaderboard_entries_proto.sync_detailed,
        client=client,
        project_identifier=project_identifier,
        type=["run"],
        body=body,
    )

    return ProtoLeaderboardEntriesSearchResultDTO.FromString(response.content)


def _process_experiment_page(
    data: ProtoLeaderboardEntriesSearchResultDTO,
) -> util.Page[ExperimentSysAttrs]:
    items = []
    for entry in data.entries:
        attributes = {attr.name: attr.string_properties.value for attr in entry.attributes}
        item = ExperimentSysAttrs(
            sys_name=identifiers.SysName(attributes["sys/name"]), sys_id=identifiers.SysId(attributes["sys/id"])
        )
        items.append(item)
    return util.Page(items=items)


def _make_new_experiment_page_params(
    params: dict[str, Any],
    data: Optional[ProtoLeaderboardEntriesSearchResultDTO],
    batch_size: int,
    limit: Optional[int],
) -> Optional[dict[str, Any]]:

    if data is None:
        params["pagination"]["offset"] = 0
        if limit is not None:
            params["pagination"]["limit"] = min(limit, batch_size)
        return params

    if len(data.entries) < batch_size:
        return None

    params["pagination"]["offset"] += batch_size
    if limit is not None:
        offset = params["pagination"]["offset"]
        params["pagination"]["limit"] = min(limit - offset, batch_size)

    return params
