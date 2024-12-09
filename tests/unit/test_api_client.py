from datetime import (
    datetime,
    timezone,
)
from typing import (
    List,
    Tuple,
)
from unittest.mock import (
    Mock,
    patch,
)

from neptune_retrieval_api.proto.neptune_pb.api.v1.model.series_values_pb2 import (
    ProtoFloatPointValueDTO,
    ProtoFloatSeriesValuesDTO,
    ProtoFloatSeriesValuesResponseDTO,
    ProtoFloatSeriesValuesSingleSeriesResponseDTO,
)
from pytest import fixture

from neptune_fetcher.api.api_client import ApiClient
from neptune_fetcher.fields import FloatPointValue


@fixture
def get_multiple_float_series_values_proto():
    with patch("neptune_retrieval_api.api.default.get_multiple_float_series_values_proto.sync_detailed") as patched:
        yield patched


class TestApiClient(ApiClient):
    def __init__(self):
        # don't call super().__init__ to avoid an attempt to authenticate
        self._backend = None


def response(body: ProtoFloatSeriesValuesResponseDTO, status_code: int = 200):
    content = body.SerializeToString()
    return Mock(status_code=Mock(value=status_code), content=content)


def values_model(steps_values: List[Tuple[float, float]]) -> List[FloatPointValue]:
    return [
        FloatPointValue(
            timestamp=datetime.fromtimestamp(i / 1000.0, tz=timezone.utc),
            step=step,
            value=value,
        )
        for i, (step, value) in enumerate(steps_values)
    ]


def values_dto(steps_values: List[Tuple[float, float]]) -> [ProtoFloatPointValueDTO]:
    return [
        ProtoFloatPointValueDTO(
            timestamp_millis=i,
            step=step,
            value=value,
        )
        for i, (step, value) in enumerate(steps_values)
    ]


def single_series_dto(
    steps_values: List[Tuple[float, float]], request_id: str = "0"
) -> ProtoFloatSeriesValuesSingleSeriesResponseDTO:
    return ProtoFloatSeriesValuesSingleSeriesResponseDTO(
        requestId=request_id,
        series=ProtoFloatSeriesValuesDTO(
            total_item_count=len(steps_values),
            values=values_dto(steps_values),
        ),
    )


def multiple_series_dto(
    steps_values: List[List[Tuple[float, float]]],
) -> ProtoFloatSeriesValuesResponseDTO:
    return ProtoFloatSeriesValuesResponseDTO(
        series=[single_series_dto(steps_values, request_id=str(i)) for i, steps_values in enumerate(steps_values)]
    )


def test_fetch_multiple_series_values__single_path__returns_empty_series(get_multiple_float_series_values_proto):
    api_client = TestApiClient()
    return_value = response(multiple_series_dto([[]]))
    get_multiple_float_series_values_proto.return_value = return_value

    results = api_client.fetch_multiple_series_values(
        paths=["path1"],
        include_inherited=True,
        container_id="container_id",
    )
    results = dict(results)

    assert results["path1"] == []


def test_fetch_multiple_series_values__single_path__returns_values(get_multiple_float_series_values_proto):
    api_client = TestApiClient()
    values = [(step, step * 2) for step in range(10)]
    return_value = response(multiple_series_dto([values]))
    get_multiple_float_series_values_proto.return_value = return_value

    results = api_client.fetch_multiple_series_values(
        paths=["path1"],
        include_inherited=True,
        container_id="container_id",
    )
    results = dict(results)

    assert results["path1"] == values_model(steps_values=values)


def test_fetch_multiple_series_values__multiple_paths__returns_values(get_multiple_float_series_values_proto):
    api_client = TestApiClient()
    paths = ["path1", "path2", "path3"]
    values = [[(step, step * (10**i)) for step in range(10)] for i in range(len(paths))]
    return_value = response(multiple_series_dto(values))
    get_multiple_float_series_values_proto.return_value = return_value

    results = api_client.fetch_multiple_series_values(
        paths=paths,
        include_inherited=True,
        container_id="container_id",
    )
    results = dict(results)

    assert results["path1"] == values_model(steps_values=values[0])
    assert results["path2"] == values_model(steps_values=values[1])
    assert results["path3"] == values_model(steps_values=values[2])


def test_fetch_multiple_series_values__single_path__returns_values_exceeding_batch(
    get_multiple_float_series_values_proto,
):
    api_client = TestApiClient()
    values = [(step, step * 2) for step in range(2_300_000)]
    get_multiple_float_series_values_proto.side_effect = [
        response(multiple_series_dto([batch]))
        for batch in [values[:1_000_000], values[1_000_000:2_000_000], values[2_000_000:]]
    ]

    results = api_client.fetch_multiple_series_values(
        paths=["path1"],
        include_inherited=True,
        container_id="container_id",
    )
    result = dict(results)

    assert len(result["path1"]) == len(values)
    expected_values = (
        values_model(steps_values=values[:1_000_000])
        + values_model(steps_values=values[1_000_000:2_000_000])
        + values_model(steps_values=values[2_000_000:])
    )
    for value, expected in zip(result["path1"], expected_values):
        assert value == expected
