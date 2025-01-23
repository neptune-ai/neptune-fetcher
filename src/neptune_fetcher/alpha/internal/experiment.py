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

from typing import (
    Any,
    Generator,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import search_leaderboard_entries_proto
from neptune_retrieval_api.models import SearchLeaderboardEntriesParamsDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)

from neptune_fetcher.alpha.filter import ExperimentFilter
from neptune_fetcher.alpha.internal import util

_DEFAULT_BATCH_SIZE = 10_000


def find_experiments(
    client: AuthenticatedClient,
    project_id: str,
    experiment_filter: Union[str, ExperimentFilter, None] = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> Generator[util.Page[str], None, None]:
    params: dict[str, Any] = {
        "attributeFilters": [{"path": "sys/name"}],
        "pagination": {"limit": batch_size},
        "experimentLeader": True,
    }
    if experiment_filter is not None:
        params["query"] = {"query": str(experiment_filter)}

    offset = 0
    while True:
        params["pagination"]["offset"] = offset

        body = SearchLeaderboardEntriesParamsDTO.from_dict(params)

        response = util.backoff_retry(
            search_leaderboard_entries_proto.sync_detailed,
            client=client,
            project_identifier=project_id,
            type=["run"],
            body=body,
        )

        data: ProtoLeaderboardEntriesSearchResultDTO = ProtoLeaderboardEntriesSearchResultDTO.FromString(
            response.content
        )

        items = [entry.attributes[0].string_properties.value for entry in data.entries]
        yield util.Page(items=items)

        if len(data.entries) < batch_size:
            break
        offset += batch_size
