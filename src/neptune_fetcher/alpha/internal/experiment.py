from typing import Union

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import (
    search_leaderboard_entries_proto,
)
from neptune_retrieval_api.models import (
    SearchLeaderboardEntriesParamsDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)

from .retry import backoff_retry


_DEFAULT_BATCH_SIZE = 10_000


def find_experiments(
        client: AuthenticatedClient,
        project_id: str,
        experiment_filter: Union[str, None] = None,  # TODO: Add Filter class
        batch_size: int = _DEFAULT_BATCH_SIZE,
) -> list[str]:

    params = {
            "attributeFilters": [{"path": "sys/name"}],
            "pagination": {"limit": batch_size},
            "experimentLeader": True,
        }
    if experiment_filter:
            params["query"] = {"query": experiment_filter}

    result = []
    offset = 0
    while True:
        params["pagination"]["offset"] = offset

        body = SearchLeaderboardEntriesParamsDTO.from_dict(params)

        response = backoff_retry(
            lambda: search_leaderboard_entries_proto.sync_detailed(
                client=client, project_identifier=project_id, type=["run"], body=body
            )
        )

        data: ProtoLeaderboardEntriesSearchResultDTO = ProtoLeaderboardEntriesSearchResultDTO.FromString(response.content)

        result.extend(entry.attributes[0].string_properties.value for entry in data.entries)

        if len(data.entries) < batch_size:
            break
        offset += batch_size

    return result