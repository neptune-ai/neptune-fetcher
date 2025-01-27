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
def set_envs(monkeypatch):
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "default_project")
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "default_token")


def test_defaults_from_env():
    ctx = get_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    assert npt.set_context() == get_context()


def test_no_env_configured(monkeypatch):
    # No API token set
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    with pytest.raises(ValueError) as exc:
        get_context()
        exc.match("Unable to determine Neptune API token")

    # No project set
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "default_token")
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)

    with pytest.raises(ValueError) as exc:
        get_context()
        exc.match("Unable to determine Neptune project")

    # No project nor token set
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    with pytest.raises(ValueError) as exc:
        get_context()
        exc.match("Unable to determine Neptune")


def test_set_project():
    ctx = npt.set_project("my_project")
    assert ctx.project == "my_project"
    assert ctx.api_token == "default_token"

    assert get_context() == ctx

    ctx = get_context(Context(project="another_project"))
    assert ctx.project == "another_project"
    assert ctx.api_token == "default_token"

    ctx = npt.set_project("my_project2")
    assert ctx.project == "my_project2"
    assert ctx.api_token == "default_token"

    ctx = npt.set_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    # Sanity check
    with pytest.raises(ValueError):
        npt.set_project("")
        npt.set_project(None)


def test_set_api_token():
    ctx = npt.set_api_token("my_token")
    assert ctx.project == "default_project"
    assert ctx.api_token == "my_token"

    assert get_context() == ctx

    ctx = get_context(Context(api_token="another_token"))
    assert ctx.project == "default_project"
    assert ctx.api_token == "another_token"

    ctx = npt.set_api_token("my_token2")
    assert ctx.project == "default_project"
    assert ctx.api_token == "my_token2"

    ctx = npt.set_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    # Sanity check
    with pytest.raises(ValueError):
        npt.set_project("")
        npt.set_project(None)


def test_reset_context_to_defaults():
    npt.set_api_token("my_token")
    npt.set_project("my_project")

    ctx = get_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    npt.set_context()
    ctx = get_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"


def test_get_context_with_user_context():
    ctx = get_context(Context())
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    ctx = get_context(Context(project="my_project"))
    assert ctx.project == "my_project"
    assert ctx.api_token == "default_token"

    ctx = get_context(Context(api_token="my_token"))
    assert ctx.project == "default_project"
    assert ctx.api_token == "my_token"

    ctx = get_context(Context(project="my_project", api_token="my_token"))
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"


def test_get_context_with_set_project_and_set_api_token():
    npt.set_project("my_project")
    npt.set_api_token("my_token")

    ctx = get_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    ctx = get_context(Context(project="another_project"))
    assert ctx.project == "another_project"
    assert ctx.api_token == "my_token"

    ctx = get_context(Context(api_token="another_token"))
    assert ctx.project == "my_project"
    assert ctx.api_token == "another_token"

    # Back to defaults
    npt.set_context()
    ctx = get_context()
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    ctx = get_context(Context(project="another_project", api_token="another_token"))
    assert ctx.project == "another_project"
    assert ctx.api_token == "another_token"
