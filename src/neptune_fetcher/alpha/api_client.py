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

__all__ = ["AuthenticatedClientBuilder"]

import logging
from typing import (
    Dict,
    Optional,
)

from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials

from ..utilities.net import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from .context import Context
from .internal.context import get_local_or_global_context

# Disable httpx logging, httpx logs requests at INFO level
logging.getLogger("httpx").setLevel(logging.WARN)


class AuthenticatedClientBuilder:
    cache: dict[int, AuthenticatedClient] = {}

    @classmethod
    def build(cls, context: Optional[Context] = None, proxies: Optional[Dict[str, str]] = None) -> AuthenticatedClient:
        api_token = get_local_or_global_context(context).api_token
        hash_key = hash((api_token, proxies))

        if hash_key in cls.cache:
            return cls.cache[hash_key]

        credentials = Credentials.from_api_key(api_key=api_token)
        config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=proxies)
        client = create_auth_api_client(
            credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=proxies
        )

        cls.cache[hash_key] = client

        return client

    @classmethod
    def clear_cache(cls) -> None:
        cls.cache.clear()
