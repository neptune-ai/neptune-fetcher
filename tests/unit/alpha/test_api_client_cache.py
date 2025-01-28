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
#
import base64
import json
import random
from unittest.mock import (
    Mock,
    patch,
)

from pytest import fixture

import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.internal import env
from neptune_fetcher.alpha.internal.api_client import (
    clear_cache,
    get_client,
)

# Caching logic being tested: only API token and proxies are used as part of the cache key. Projects don't matter.


def make_token():
    """We need a token that can be deserialized"""

    rnd = random.random()
    return base64.b64encode(
        json.dumps({"api_address": f"https://{rnd}.does-not-exist", "api_url": f"https://{rnd}.does-not-exist"}).encode(
            "utf-8"
        )
    ).decode("utf-8")


@fixture
def default_token():
    return make_token()


@fixture(autouse=True)
def set_default_envs(monkeypatch, default_token):
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "default_project")
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, default_token)


@fixture(autouse=True)
def clear_cache_after_test():
    yield
    clear_cache()


@fixture(autouse=True)
def mock_networking():
    with (
        patch("neptune_fetcher.alpha.internal.api_client.get_config_and_token_urls") as get_config_and_token_urls,
        patch("neptune_fetcher.alpha.internal.api_client.create_auth_api_client") as create_auth_api_client,
    ):
        get_config_and_token_urls.return_value = (None, None)
        # create_auth_api_client() needs to return a different object each time (a new client is created each time)
        create_auth_api_client.side_effect = lambda *args, **kwargs: Mock()
        yield


def test_same_token(default_token):
    client = get_client()
    assert get_client() is client
    assert get_client(Context(api_token=default_token)) is client

    npt.set_api_token(make_token())
    assert get_client(Context(api_token=default_token)) is client


def test_same_token_different_project(default_token):
    client = get_client()
    assert get_client(Context(project="foo")) is client
    assert get_client(Context(project="foo", api_token=default_token)) is client

    npt.set_project("foo")
    assert get_client() is client


def test_different_token(default_token):
    client = get_client()
    assert get_client(Context(api_token=make_token())) is not client

    npt.set_api_token(make_token())
    assert get_client() is not client


def test_same_token_and_proxies(default_token):
    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(proxies=proxies)
    assert get_client(proxies=proxies) is client
    assert get_client(Context(api_token=default_token), proxies=proxies) is client


def test_same_token_different_proxies(default_token):
    proxies1 = {"https": "https://proxy1.does-not-exist"}
    proxies2 = {"https": "https://proxy2.does-not-exist"}

    assert get_client(proxies=proxies1) is not get_client(proxies=proxies2)
    assert get_client(Context(api_token=default_token), proxies=proxies1) is not get_client(proxies=proxies2)
    assert get_client(proxies=proxies1) is not get_client(Context(api_token=default_token), proxies=proxies2)


def test_same_proxies_different_token(default_token):
    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(proxies=proxies)
    assert get_client(Context(api_token=make_token()), proxies=proxies) is not client

    npt.set_api_token(make_token())
    assert get_client(proxies=proxies) is not client
