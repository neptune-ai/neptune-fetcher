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
from neptune_fetcher.alpha.internal import env


@fixture(autouse=True)
def default_ctx(monkeypatch):
    monkeypatch.setenv(env.NEPTUNE_PROJECT.name, "default_project")
    monkeypatch.setenv(env.NEPTUNE_API_TOKEN.name, "default_token")

    # Basic sanity checks before each test:
    # - setting context returns the new context
    ctx = npt.set_context()
    assert get_context() == ctx

    # - default context must pick up env variables
    assert ctx.project == "default_project"
    assert ctx.api_token == "default_token"

    # - and must be valid
    validate_context(ctx)

    return ctx


def test_context_factory_methods(default_ctx):
    assert default_ctx.with_project("my_project") == Context(project="my_project", api_token=default_ctx.api_token)
    assert default_ctx.with_api_token("my_token") == Context(project=default_ctx.project, api_token="my_token")


def test_set_context(default_ctx):
    npt.set_context(Context(project="my_project", api_token="my_token"))
    ctx = get_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == "my_token"
    validate_context(ctx)

    npt.set_context()
    assert get_context() == default_ctx


def test_set_context_with_project(default_ctx):
    npt.set_context(default_ctx.with_project("foo"))
    ctx = get_context()
    assert ctx.project == "foo"
    assert ctx.api_token == default_ctx.api_token
    validate_context(ctx)

    npt.set_context()
    assert get_context() == default_ctx


def test_set_context_with_api_token(default_ctx):
    npt.set_context(default_ctx.with_api_token("bar"))
    ctx = get_context()
    assert ctx.project == default_ctx.project
    assert ctx.api_token == "bar"
    validate_context(ctx)

    npt.set_context()
    assert get_context() == default_ctx


def test_set_project(monkeypatch, default_ctx):
    npt.set_project("my_project")
    ctx = get_context()
    assert ctx.project == "my_project"
    assert ctx.api_token == default_ctx.api_token
    validate_context(ctx)

    # Make the default context have project = None
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    npt.set_context()

    npt.set_project("another_project")
    ctx = get_context()
    assert ctx.project == "another_project"
    assert ctx.api_token == default_ctx.api_token
    validate_context(ctx)

    with pytest.raises(ValueError):
        npt.set_project("")

    with pytest.raises(ValueError):
        npt.set_project(None)


def test_set_api_token(monkeypatch, default_ctx):
    npt.set_api_token("my_token")
    ctx = get_context()
    assert ctx.project == default_ctx.project
    assert ctx.api_token == "my_token"
    validate_context(ctx)

    # Make the default context have api_token = None
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    npt.set_api_token("another_token")
    ctx = get_context()
    assert ctx.project == default_ctx.project
    assert ctx.api_token == "another_token"
    validate_context(ctx)

    with pytest.raises(ValueError):
        npt.set_api_token("")

    with pytest.raises(ValueError):
        npt.set_api_token(None)


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


def test_env_no_api_token_fails_validation(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        validate_context(get_context())

    exc.match("Unable to determine Neptune API token")


def test_env_no_project_fails_validation(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        validate_context(get_context())

    exc.match("Unable to determine Neptune project name")


def test_no_env_configured_raises(monkeypatch):
    monkeypatch.delenv(env.NEPTUNE_PROJECT.name)
    monkeypatch.delenv(env.NEPTUNE_API_TOKEN.name)
    npt.set_context()

    with pytest.raises(ValueError) as exc:
        validate_context(get_context())

    exc.match("Unable to determine Neptune")


def test_validate_invalid_context():
    with pytest.raises(ValueError) as exc:
        validate_context(Context())
    exc.match("Unable to determine Neptune")

    with pytest.raises(ValueError) as exc:
        validate_context(Context(project="foo"))
    exc.match("Unable to determine Neptune API token")

    with pytest.raises(ValueError) as exc:
        validate_context(Context(api_token="bar"))
    exc.match("Unable to determine Neptune project name")
