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

from neptune_fetcher.internal.client import clear_cache
from neptune_fetcher.internal.client import get_client as _get_client
from neptune_fetcher.internal.context import Context


def get_client(context: Context, proxies: dict = None):
    return _get_client(context=context, proxies=proxies)


# Caching logic being tested: only API token and proxies are used as part of the cache key. Projects don't matter.


# This hook is called by pytest on test collection. We force all tests in this file to
# be run in a single process, so that we can test the cache properly: eg. don't cover up
# lingering data in the cache by launching some tests in a separate address space.
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
def context():
    return Context(api_token=make_token(), project="default_project")


@fixture(autouse=True)
def clear_cache_before_test():
    clear_cache()
    yield


@fixture(autouse=True)
def mock_networking():
    with (
        patch("neptune_fetcher.internal.client.get_config_and_token_urls") as get_config_and_token_urls,
        patch("neptune_fetcher.internal.client.create_auth_api_client") as create_auth_api_client,
    ):
        get_config_and_token_urls.return_value = (Mock(), Mock())
        # create_auth_api_client() needs to return a different "client" each time
        create_auth_api_client.side_effect = lambda *args, **kwargs: Mock()
        yield


def test_same_token(context):
    """Should return a single instance of the client because the token is the same"""

    client = get_client(context)
    assert get_client(context) is client


def test_same_token_different_project(context):
    """Should return a single instance of the client because the token is the same"""

    client = get_client(context)
    assert get_client(context.with_project("foo")) is client


def test_different_token(context):
    """Should return a different instance of the client, depending on the token"""

    client = get_client(context)
    assert get_client(context.with_api_token(make_token())) is not client


def test_same_token_and_proxies(context):
    """Should return a single instance of the client because the token and proxies are the same"""
    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(context, proxies=proxies)
    assert get_client(context, proxies=proxies) is client
    assert get_client(context.with_project("foo"), proxies=proxies) is client


def test_same_token_different_proxies(context):
    """Should return a different instance of the client because the proxies are different"""
    proxies1 = {"https": "https://proxy1.does-not-exist"}
    proxies2 = {"https": "https://proxy2.does-not-exist"}

    assert get_client(context, proxies=proxies1) is not get_client(context, proxies=proxies2)


def test_same_proxies_different_token(context):
    """Should return a different instance of the client because the API token is different"""

    proxies = {"https": "https://proxy.does-not-exist"}

    client = get_client(context, proxies=proxies)
    assert get_client(context.with_api_token(make_token()), proxies=proxies) is not client
