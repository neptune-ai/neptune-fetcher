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
#
import pytest
from pytest import fixture

import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.internal import env
from neptune_fetcher.alpha.internal.util import get_context


@fixture(autouse=True)
def set_envs_and_reset_context(monkeypatch):
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "default_project")
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "default_token")

    npt.set_context()


def test_set_context_returns_the_new_context():
    ctx = npt.set_context(Context("my_project", "my_token"))
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    assert get_context() == ctx


def test_defaults_from_env(monkeypatch):
    ctx = get_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"


def test_env_no_api_token_raises(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        get_context()

    exc.match("Unable to determine Neptune API token")


def test_env_no_project_raises(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        get_context()

    exc.match("Unable to determine Neptune project")


def test_no_env_configured_raises(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        get_context()

    exc.match("Unable to determine Neptune")


def test_set_project(monkeypatch):
    ctx = npt.set_project("my_project")
    assert ctx.project == "my_project"
    assert ctx.api_token == "default_token"

    assert get_context() == ctx

    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    npt.set_context()

    ctx = npt.set_project("another_project")
    assert ctx.project == "another_project"
    assert ctx.api_token == "default_token"

    with pytest.raises(ValueError):
        npt.set_project("")
        npt.set_project(None)


def test_set_api_token(monkeypatch):
    ctx = npt.set_api_token("my_token")
    assert ctx.project == "default_project"
    assert ctx.api_token == "my_token"

    assert get_context() == ctx

    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    ctx = npt.set_api_token("another_token")
    assert ctx.project == "default_project"
    assert ctx.api_token == "another_token"

    with pytest.raises(ValueError):
        npt.set_api_token("")
        npt.set_api_token(None)


def test_reset_context_to_defaults():
    npt.set_api_token("my_token")
    npt.set_project("my_project")

    ctx = get_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    ctx = npt.set_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"


def test_get_context_with_user_context():
    ctx = get_context(Context(project="my_project", api_token="my_token"))
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    with pytest.raises(ValueError):
        get_context(Context())

    with pytest.raises(ValueError):
        get_context(Context(project="my_project"))

    with pytest.raises(ValueError):
        get_context(Context(api_token="my_token"))
