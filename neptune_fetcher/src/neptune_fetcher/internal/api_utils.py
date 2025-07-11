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

from dataclasses import dataclass
from http import HTTPStatus
from typing import (
    Callable,
    Dict,
    Optional,
)

import httpx
from neptune_api import (
    AuthenticatedClient,
    Client,
)
from neptune_api.api.backend import get_client_config
from neptune_api.auth_helpers import exchange_api_key
from neptune_api.credentials import Credentials
from neptune_api.models import ClientConfig
from neptune_api.types import Response

from ..exceptions import NeptuneFailedToFetchClientConfig
from .env import (
    NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS,
    NEPTUNE_VERIFY_SSL,
)
from .retrieval.retry import handle_errors_default


@dataclass
class TokenRefreshingURLs:
    authorization_endpoint: str
    token_endpoint: str

    @classmethod
    def from_dict(cls, data: dict) -> "TokenRefreshingURLs":
        return TokenRefreshingURLs(
            authorization_endpoint=data["authorization_endpoint"], token_endpoint=data["token_endpoint"]
        )


def _wrap_httpx_json_response(httpx_response: httpx.Response) -> Response:
    """Wrap a httpx.Response into an neptune-api Response object that is compatible
    with backoff_retry(). Use .json() as parsed content in the result."""

    return Response(
        status_code=HTTPStatus(httpx_response.status_code),
        content=httpx_response.content,
        headers=httpx_response.headers,
        parsed=httpx_response.json(),
    )


def get_config_and_token_urls(
    *, credentials: Credentials, proxies: Optional[Dict[str, str]] = None
) -> tuple[ClientConfig, TokenRefreshingURLs]:
    timeout = httpx.Timeout(NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS.get())
    with Client(
        base_url=credentials.base_url,
        httpx_args={"mounts": proxies},
        verify_ssl=NEPTUNE_VERIFY_SSL.get(),
        timeout=timeout,
        headers={"User-Agent": _generate_user_agent()},
    ) as client:
        try:
            config_response = handle_errors_default(get_client_config.sync_detailed)(client=client)
            config = config_response.parsed

            urls_response = handle_errors_default(
                lambda: _wrap_httpx_json_response(client.get_httpx_client().get(config.security.open_id_discovery))
            )()
            token_urls = TokenRefreshingURLs.from_dict(urls_response.parsed)

            return config, token_urls
        except Exception as e:
            raise NeptuneFailedToFetchClientConfig(exception=e) from e


def create_auth_api_client(
    *,
    credentials: Credentials,
    config: ClientConfig,
    token_refreshing_urls: TokenRefreshingURLs,
    proxies: Optional[Dict[str, str]] = None,
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        client_id=config.security.client_id,
        token_refreshing_endpoint=token_refreshing_urls.token_endpoint,
        api_key_exchange_callback=exchange_api_key,
        verify_ssl=NEPTUNE_VERIFY_SSL.get(),
        httpx_args={"mounts": proxies, "http2": False},
        timeout=httpx.Timeout(NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS.get()),
        headers={"User-Agent": _generate_user_agent()},
    )


_ILLEGAL_CHARS = str.maketrans({c: "_" for c in " ();/"})


def _generate_user_agent() -> str:
    import platform
    from importlib.metadata import version

    def sanitize(value: Callable[[], str]) -> str:
        try:
            result = value()
            return result.translate(_ILLEGAL_CHARS)
        except Exception:
            return "unknown"

    package_name = "neptune-fetcher"
    package_version = sanitize(lambda: version(package_name))
    additional_metadata = {
        "neptune-api": sanitize(lambda: version("neptune-api")),
        "python": sanitize(platform.python_version),
        "os": sanitize(platform.system),
    }

    additional_metadata_str = "; ".join(f"{k}={v}" for k, v in additional_metadata.items())
    return f"{package_name}/{package_version} ({additional_metadata_str})"
