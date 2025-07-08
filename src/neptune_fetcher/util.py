#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
#
import functools as ft
import logging
import os
import warnings
from dataclasses import dataclass
from http import HTTPStatus
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    TypeVar,
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

from neptune_fetcher.exceptions import NeptuneError
from neptune_fetcher.internal.env import (
    NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS,
    NEPTUNE_VERIFY_SSL,
)
from neptune_fetcher.internal.retrieval.retry import handle_errors_default

logger = logging.getLogger(__name__)

# Disable httpx logging, httpx logs requests at INFO level
logging.getLogger("httpx").setLevel(logging.WARN)

T = TypeVar("T")


def getenv_int(name: str, default: int, *, positive=True) -> int:
    value = os.environ.get(name)
    if value is None:
        return default

    try:
        value = int(value)
        if positive and value <= 0:
            raise ValueError
    except ValueError:
        raise ValueError(f"Environment variable {name} must be a positive integer, got '{value}'")

    return value


class NeptuneException(Exception):
    def __eq__(self, other: Any) -> bool:
        if type(other) is type(self):
            return super().__eq__(other) and str(self).__eq__(str(other))
        else:
            return False

    def __hash__(self) -> int:
        return hash((super().__hash__(), str(self)))


class NeptuneWarning(Warning):
    pass


warnings.simplefilter("once", category=NeptuneWarning)


def escape_nql_criterion(criterion):
    """
    Escape backslash and (double-)quotes in the string, to match what the NQL engine expects.
    """

    return criterion.replace("\\", r"\\").replace('"', r"\"")


# We keep a set of types we've warned the user about to make sure we warn about a type only once.
# This is necessary because of a bug in pandas, that causes duplicate warnings to be issued everytime after an
# DataFrame() is created (presumably only empty DF).
# The bug basically makes `warnings.simplefilter("once", NeptuneWarning)` not work as expected, and would flood
# the user with warnings in some cases.
_warned_types = set()


def warn_unsupported_value_type(type_: str) -> None:
    if type_ in _warned_types:
        return

    _warned_types.add(type_)
    warnings.warn(
        f"A value of type `{type_}` was returned by your query. This type is not supported by your installed version "
        "of neptune-fetcher. Values will evaluate to `None` and empty DataFrames. "
        "Upgrade neptune-fetcher to access this data.",
        NeptuneWarning,
    )


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
    *, credentials: Credentials, proxies: Optional[Dict[str, str]]
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
            raise NeptuneException(f"Failed to fetch client configuration: {e}") from e


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


def batched_paths(paths: list[str], batch_size: int, query_size_limit: int) -> list[list[str]]:
    """
    Split the provided list of attribute paths into batches such that:
     * the number of items in a batch does not exceed `batch_size`
     * the sum of lengths of paths in a batch does not exceed `query_size_limit`
    """

    batches = []
    current_batch: list[str] = []
    current_batch_size = 0

    for path in paths:
        path_size = len(path.encode("utf8"))

        if current_batch and (len(current_batch) >= batch_size or current_batch_size + path_size > query_size_limit):
            batches.append(current_batch)
            current_batch = []
            current_batch_size = 0

        current_batch.append(path)
        current_batch_size += path_size

    if current_batch:
        batches.append(current_batch)

    return batches


def rethrow_neptune_error(func: Callable[..., T]) -> Callable[..., T]:
    @ft.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except NeptuneError as e:
            raise NeptuneException(str(e)).with_traceback(e.__traceback__) from e.__cause__

    return wrapper
