from unittest.mock import (
    Mock,
    patch,
)

from google.protobuf.message import Message
from neptune_retrieval_api.models import (
    FloatTimeSeriesValuesRequest,
    SearchLeaderboardEntriesParamsDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO
from pytest import fixture

from neptune_fetcher.api.client import NeptuneApiClient


@fixture
def search_leaderboard_entries_proto():
    with patch("neptune_retrieval_api.api.default.search_leaderboard_entries_proto.sync_detailed") as patched:
        yield patched


@fixture
def get_multiple_float_series_values_proto():
    with patch("neptune_retrieval_api.api.default.get_multiple_float_series_values_proto.sync_detailed") as patched:
        yield patched


def response(body: Message, status_code: int = 200):
    content = body.SerializeToString()
    return Mock(status_code=Mock(value=status_code), content=content)


def search_entries_params_dto() -> SearchLeaderboardEntriesParamsDTO:
    return SearchLeaderboardEntriesParamsDTO()


def search_entries_result_dto() -> ProtoLeaderboardEntriesSearchResultDTO:
    return ProtoLeaderboardEntriesSearchResultDTO()


def float_series_values_response_dto() -> ProtoFloatSeriesValuesResponseDTO:
    return ProtoFloatSeriesValuesResponseDTO()


def float_series_values_request() -> FloatTimeSeriesValuesRequest:
    return FloatTimeSeriesValuesRequest(per_series_points_limit=1000, requests=[])


def test_search_entries__returns_dto(search_leaderboard_entries_proto):
    client = NeptuneApiClient(auth_client=Mock())
    params_dto = search_entries_params_dto()
    result_dto = search_entries_result_dto()
    search_leaderboard_entries_proto.return_value = response(result_dto)

    actual = client.search_entries(project_id="project_id", types=["run"], body=params_dto)

    assert actual == result_dto


def test_get_float_series_values__returns_dto(get_multiple_float_series_values_proto):
    client = NeptuneApiClient(auth_client=Mock())
    params_dto = float_series_values_request()
    result_dto = float_series_values_response_dto()
    get_multiple_float_series_values_proto.return_value = response(result_dto)

    actual = client.get_float_series_values(body=params_dto)

    assert actual == result_dto
