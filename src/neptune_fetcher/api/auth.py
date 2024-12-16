from dataclasses import dataclass
from typing import (
    Mapping,
    Union,
)

import neptune_api.client
from neptune_api.api.backend import (
    exchange_api_token,
    get_client_config,
)
from neptune_api.credentials import Credentials
from neptune_api.errors import UnableToExchangeApiKeyError
from neptune_api.models import (
    Error,
    NeptuneOauthToken,
)
from neptune_api.types import OAuthToken

from ..env import NEPTUNE_VERIFY_SSL

__all__ = ("create_authenticated_client",)


def create_authenticated_client(api_token: str, proxies: Mapping[str, str]) -> neptune_api.client.AuthenticatedClient:
    credentials = Credentials.from_api_key(api_key=api_token)
    verify_ssl = NEPTUNE_VERIFY_SSL.get()

    with neptune_api.client.Client(
        base_url=credentials.base_url, httpx_args={"mounts": proxies}, verify_ssl=verify_ssl
    ) as client:
        config = get_client_config.sync(client=client)
        if config is None or isinstance(config, Error):
            raise RuntimeError(f"Failed to get client config: {config}")
        response = client.get_httpx_client().get(config.security.open_id_discovery)
        refresh_urls = _TokenRefreshURLs.from_dict(response.json())

    return neptune_api.client.AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=refresh_urls.token_endpoint,
        api_key_exchange_callback=_exchange_api_key,
        httpx_args={"mounts": proxies, "http2": False},
        verify_ssl=verify_ssl,
    )


@dataclass
class _TokenRefreshURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> "_TokenRefreshURLs":
        return _TokenRefreshURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def _exchange_api_key(client: neptune_api.client.Client, credentials: Credentials) -> OAuthToken:
    token_data: Union[NeptuneOauthToken, None, Error] = exchange_api_token.sync(
        client=client, x_neptune_api_token=credentials.api_key
    )

    if isinstance(token_data, Error):
        raise UnableToExchangeApiKeyError(reason=str(token_data.message))
    if not token_data:
        raise UnableToExchangeApiKeyError()

    return OAuthToken.from_tokens(access=token_data.access_token, refresh=token_data.refresh_token)
