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
from neptune_fetcher.alpha.context import (
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.exception import (
    NeptuneApiTokenNotProvided,
    NeptuneProjectNotProvided,
)
from neptune_fetcher.alpha.internal import env


@fixture
def default_ctx():
    return Context(project="default_project", api_token="default_token")


def test_context_factory_methods(default_ctx):
    assert default_ctx.with_project("my_project") == Context(project="my_project", api_token="default_token")
    assert default_ctx.with_api_token("my_token") == Context(project="default_project", api_token="my_token")


def test_set_context(default_ctx):
    # Full context provided
    new_ctx = npt.set_context(default_ctx)
    ctx = get_context()
    assert ctx == new_ctx, "set_context() did not return the new context"
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    # Project only
    ctx = npt.set_context(Context(project="my_project"))
    assert ctx.project == "my_project"
    assert ctx.api_token is None

    # API token only
    ctx = npt.set_context(Context(api_token="my_token"))
    assert ctx.project is None
    assert ctx.api_token == "my_token"

    # Empty context
    ctx = npt.set_context(Context())
    assert ctx.project is None
    assert ctx.api_token is None

    # Case for npt.set_context(None) is covered in test_set_context_from_envs


def test_set_project(default_ctx):
    npt.set_context(default_ctx)

    new_ctx = npt.set_project("my_project")
    ctx = get_context()
    assert ctx == new_ctx, "set_project() did not return the new context"
    assert ctx.project == "my_project"
    assert ctx.api_token == "default_token"

    with pytest.raises(ValueError):
        npt.set_project("")

    with pytest.raises(ValueError):
        npt.set_project(None)


def test_set_api_token(default_ctx):
    npt.set_context(default_ctx)

    new_ctx = npt.set_api_token("my_token")
    ctx = get_context()
    assert ctx == new_ctx, "set_api_token() did not return the new context"
    assert ctx.project == "default_project"
    assert ctx.api_token == "my_token"

    with pytest.raises(ValueError):
        npt.set_api_token("")

    with pytest.raises(ValueError):
        npt.set_api_token(None)


def test_context_from_envs(monkeypatch):
    # No envs
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name, raising=False)
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name, raising=False)
    ctx = npt.set_context()
    assert ctx.project is None
    assert ctx.api_token is None

    # Required env provided
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "my_project")
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "my_token")
    ctx = npt.set_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"

    # Project only
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "another_project")
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    ctx = npt.set_context()
    assert ctx.project == "another_project"
    assert ctx.api_token is None

    # API token only
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "another_token")
    ctx = npt.set_context()
    assert ctx.project is None
    assert ctx.api_token == "another_token"


def test_validate_context():
    with pytest.raises(NeptuneProjectNotProvided):
        validate_context(Context())

    with pytest.raises(NeptuneApiTokenNotProvided):
        validate_context(Context(project="foo"))

    with pytest.raises(NeptuneProjectNotProvided):
        validate_context(Context(api_token="bar"))
