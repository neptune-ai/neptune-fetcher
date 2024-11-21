from __future__ import annotations

__all__ = ["ApiClient"]

import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Any,
    Callable,
    Dict,
    Final,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import (
    get_client_config,
    get_project,
)
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.models import (
    ClientConfig,
    Error,
    ProjectDTO,
)
from neptune_retrieval_api.api.default import (
    get_multiple_float_series_values_proto,
    query_attribute_definitions_proto,
    query_attribute_definitions_within_project,
    query_attributes_within_project_proto,
    search_leaderboard_entries_proto,
)
from neptune_retrieval_api.models import (
    AttributesHolderIdentifier,
    FloatTimeSeriesValuesRequest,
    FloatTimeSeriesValuesRequestOrder,
    FloatTimeSeriesValuesRequestSeries,
    OpenRangeDTO,
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
    QueryAttributesBodyDTO,
    SearchLeaderboardEntriesParamsDTO,
    TimeSeries,
    TimeSeriesLineage,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.attributes_pb2 import (
    ProtoAttributesSearchResultDTO,
    ProtoQueryAttributesResultDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.series_values_pb2 import ProtoFloatSeriesValuesResponseDTO
from neptune_retrieval_api.types import Response

from neptune_fetcher.fields import (
    FieldDefinition,
    FieldType,
    FloatPointValue,
)
from neptune_fetcher.util import NeptuneException

API_TOKEN_ENV_NAME: Final[str] = "NEPTUNE_API_TOKEN"
NEPTUNE_VERIFY_SSL: Final[bool] = os.environ.get("NEPTUNE_VERIFY_SSL", "1").lower() in {"1", "true"}

# Disable httpx logging, httpx logs requests at INFO level
logging.getLogger("httpx").setLevel(logging.WARN)


class ApiClient:
    def __init__(self, api_token: Optional[str] = None, proxies: Optional[Dict[str, str]] = None) -> None:
        api_token = api_token if api_token else os.getenv(API_TOKEN_ENV_NAME)
        credentials = Credentials.from_api_key(api_key=api_token)
        config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=proxies)
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
        paths: List[str],
        include_inherited: bool,
        container_id: str,
        step_range: Tuple[Union[float, None], Union[float, None]] = (None, None),
    ) -> Iterator[(str, List[FloatPointValue])]:
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

        response = backoff_retry(
            lambda: get_multiple_float_series_values_proto.sync_detailed(client=self._backend, body=request)
        )

        data: ProtoFloatSeriesValuesResponseDTO = ProtoFloatSeriesValuesResponseDTO.FromString(response.content)

        return [
            [
                FloatPointValue(
                    timestamp=datetime.fromtimestamp(point.timestamp_millis / 1000.0, tz=timezone.utc),
                    value=point.value,
                    step=point.step,
                )
                for point in series.series.values
            ]
            for series in sorted(data.series, key=lambda s: int(s.requestId))
        ]

    def query_attribute_definitions(self, container_id: str) -> List[FieldDefinition]:
        response = backoff_retry(
            lambda: query_attribute_definitions_proto.sync_detailed(
                client=self._backend,
                experiment_identifier=container_id,
            )
        )
        definitions: ProtoAttributesSearchResultDTO = ProtoAttributesSearchResultDTO.FromString(response.content)
        return [
            FieldDefinition(field_definition.name, FieldType(field_definition.type))
            for field_definition in definitions.entries
        ]

    def query_attributes_within_project(
        self, project_id: str, body: QueryAttributesBodyDTO
    ) -> ProtoQueryAttributesResultDTO:
        response = backoff_retry(
            lambda: query_attributes_within_project_proto.sync_detailed(
                client=self._backend, body=body, project_identifier=project_id
            )
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

        response = backoff_retry(
            lambda: query_attribute_definitions_within_project.sync_detailed(client=self._backend, body=body)
        )

        data: QueryAttributeDefinitionsResultDTO = response.parsed

        return {t.type.value for t in data.entries}

    def search_entries(
        self, project_id: str, body: SearchLeaderboardEntriesParamsDTO
    ) -> ProtoLeaderboardEntriesSearchResultDTO:
        resp = backoff_retry(
            lambda: search_leaderboard_entries_proto.sync_detailed(
                client=self._backend, project_identifier=project_id, type=["run"], body=body
            )
        )
        proto_data = ProtoLeaderboardEntriesSearchResultDTO.FromString(resp.content)
        return proto_data

    def project_name_lookup(self, name: Optional[str] = None) -> ProjectDTO:
        response = backoff_retry(lambda: get_project.sync_detailed(client=self._backend, project_identifier=name))
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


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> TokenRefreshingURLs:
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def get_config_and_token_urls(
    *, credentials: Credentials, proxies: Optional[Dict[str, str]]
) -> tuple[ClientConfig, TokenRefreshingURLs]:
    with Client(base_url=credentials.base_url, httpx_args={"mounts": proxies}, verify_ssl=NEPTUNE_VERIFY_SSL) as client:
        config = get_client_config.sync(client=client)
        if config is None or isinstance(config, Error):
            raise RuntimeError(f"Failed to get client config: {config}")
        response = client.get_httpx_client().get(config.security.open_id_discovery)
        token_urls = TokenRefreshingURLs.from_dict(response.json())
    return config, token_urls


def create_auth_api_client(
    *,
    credentials: Credentials,
    config: ClientConfig,
    token_refreshing_urls: TokenRefreshingURLs,
    proxies: Optional[Dict[str, str]],
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
        verify_ssl=NEPTUNE_VERIFY_SSL,
        httpx_args={"mounts": proxies, "http2": False},
    )


def backoff_retry(
    func: Callable, *args, max_tries: int = 5, backoff_factor: float = 0.5, max_backoff: float = 30.0, **kwargs
) -> Response[Any]:
    """
    Retries a function with exponential backoff. The function will be called at most `max_tries` times.

    :param func: The function to retry.
    :param max_tries: Maximum number of times `func` will be called, including retries.
    :param backoff_factor: Factor by which the backoff time increases.
    :param max_backoff: Maximum backoff time.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: The result of the function call.
    """

    if max_tries < 1:
        raise ValueError("max_tries must be greater than or equal to 1")

    tries = 0
    last_exc = None
    last_response = None

    while True:
        tries += 1
        try:
            response = func(*args, **kwargs)
        except Exception as e:
            response = None
            last_exc = e

        if response is not None:
            last_response = response

            code = response.status_code.value
            if 0 <= code < 300:
                return response

            # Not a TooManyRequests or InternalServerError code
            if not (code == 429 or 500 <= code < 600):
                raise NeptuneException(f"Unexpected server response {response.status_code}: {str(response.content)}")

        if tries == max_tries:
            break

        # A retryable error occurred, back off and try again
        backoff_time = min(backoff_factor * (2**tries), max_backoff)
        time.sleep(backoff_time)

    # No more retries left
    msg = []
    if last_exc:
        msg.append(f"Last exception: {str(last_exc)}")
    if last_response:
        msg.append(f"Last response: {last_response.status_code}: {str(last_response.content)}")
    if not msg:
        raise NeptuneException("Unknown error occurred when requesting data")

    raise NeptuneException(f"Failed to get response after {tries} retries. " + "\n".join(msg))
