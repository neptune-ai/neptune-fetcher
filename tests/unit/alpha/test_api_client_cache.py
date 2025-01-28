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
import uuid
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


# This hook is called by pytest on test collection. We force all tests in this file to
# be run in a single process, so that we can test the cache properly.
def pytest_collection_modifyitems(config, items):
    for item in items:
        item.nodeid = "test_api_client_cache"


def make_token():
    """We need a token that can be deserialized"""
    rnd = str(uuid.uuid4())
    return base64.b64encode(
        json.dumps(
            {
                rnd: rnd,  # make the base64 encoded strings easier to tell apart for debugging
                "api_address": f"https://{rnd}.does-not-exist",
                "api_url": f"https:/" f"/{rnd}.does-not-exist",
            }
        ).encode("utf-8")
    ).decode("utf-8")


@fixture
def default_ctx():
    return Context(api_token=make_token(), project="default_project")


@fixture(autouse=True)
def set_default_context(monkeypatch, default_ctx):
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, default_ctx.project)
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, default_ctx.api_token)

    npt.set_context()


@fixture(autouse=True)
def clear_cache_before_test():
    clear_cache()
    yield


@fixture(autouse=True)
def mock_networking():
    with (
        patch("neptune_fetcher.alpha.internal.api_client.get_config_and_token_urls") as get_config_and_token_urls,
        patch("neptune_fetcher.alpha.internal.api_client.create_auth_api_client") as create_auth_api_client,
    ):
        get_config_and_token_urls.return_value = (Mock(), Mock())
        # create_auth_api_client() needs to return a different "client" each time
        create_auth_api_client.side_effect = lambda *args, **kwargs: Mock()
        yield


def test_same_token(default_ctx):
    client = get_client()
    assert get_client() is client
    assert get_client(Context(api_token=default_ctx.api_token, project="default_project")) is client

    ctx = default_ctx.with_api_token(make_token())
    client2 = get_client(ctx)
    assert client2 is not client

    npt.set_api_token(ctx.api_token)
    assert get_client(ctx) is client2


def test_same_token_different_project(default_ctx):
    client = get_client()
    assert get_client(default_ctx.with_project("foo")) is client

    npt.set_project("foo")
    assert get_client() is client


def test_different_token(default_ctx):
    client = get_client()
    assert get_client(default_ctx.with_api_token(make_token())) is not client

    npt.set_api_token(make_token())
    assert get_client() is not client


def test_same_token_and_proxies(default_ctx):
    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(proxies=proxies)
    assert get_client(proxies=proxies) is client
    assert get_client(default_ctx.with_project("foo"), proxies=proxies) is client


def test_same_token_different_proxies(default_ctx):
    proxies1 = {"https": "https://proxy1.does-not-exist"}
    proxies2 = {"https": "https://proxy2.does-not-exist"}

    assert get_client(proxies=proxies1) is not get_client(proxies=proxies2)

    ctx = default_ctx.with_api_token(make_token())
    assert get_client(ctx, proxies=proxies1) is not get_client(ctx, proxies=proxies2)


def test_same_proxies_different_token(default_ctx):
    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(proxies=proxies)
    assert get_client(default_ctx.with_api_token(make_token()), proxies=proxies) is not client

    npt.set_api_token(make_token())
    assert get_client(proxies=proxies) is not client
