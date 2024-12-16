from unittest.mock import Mock

from neptune_retrieval_api.models import SearchLeaderboardEntriesParamsDTO
from neptune_retrieval_api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)

from neptune_fetcher.api.fetcher import NeptuneFetcher


def search_entries_params_dto() -> SearchLeaderboardEntriesParamsDTO:
    return SearchLeaderboardEntriesParamsDTO()


def search_entries_result_dto() -> ProtoLeaderboardEntriesSearchResultDTO:
    return ProtoLeaderboardEntriesSearchResultDTO()


def test_search_entries__returns_empty_list():
    client = Mock()
    fetcher = NeptuneFetcher(project_id="project_id", client=client)
    client.search_entries.return_value = search_entries_result_dto()

    result = fetcher.list_experiments(experiments=None, limit=1000)

    assert result == []
