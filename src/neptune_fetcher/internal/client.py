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

from __future__ import annotations

__all__ = ("get_client", "clear_cache")

import logging
import threading
from typing import (
    Dict,
    Optional,
    Tuple,
)

from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials

from neptune_fetcher.internal.api_utils import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune_fetcher.internal.context import Context

# Disable httpx logging, httpx logs requests at INFO level
logging.getLogger("httpx").setLevel(logging.WARN)

_cache: dict[int, AuthenticatedClient] = {}
_lock: threading.RLock = threading.RLock()


def get_client(context: Context, proxies: Optional[Dict[str, str]] = None) -> AuthenticatedClient:
    hash_key = hash((context.api_token, _dict_to_hashable(proxies)))

    # We hold the lock during the entire client creation process, including networking.
    # This is a deliberate decision to make sure that only one client is created for a given hash_key,
    # at any given time.
    # The process can be optimized later by more granular locking (per hash_key), but for the time being
    # the decision is to keep it dead simple.

    with _lock:
        if (client := _cache.get(hash_key)) is not None:
            return client

        credentials = Credentials.from_api_key(api_key=context.api_token)
        config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=proxies)
        client = create_auth_api_client(
            credentials=credentials,
            config=config,
            token_refreshing_urls=token_urls,
            proxies=proxies,
        )

        _cache[hash_key] = client
        return client


def clear_cache() -> None:
    with _lock:
        _cache.clear()


def _dict_to_hashable(d: Optional[Dict[str, str]]) -> frozenset[Tuple[str, str]]:
    """Convert a dict to a hashable data structure"""

    items = tuple() if d is None else d.items()
    return frozenset(items)
