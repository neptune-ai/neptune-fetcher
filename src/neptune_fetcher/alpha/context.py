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


import threading
from dataclasses import dataclass
from typing import (
    Optional,
    cast,
)

from neptune_fetcher.alpha.internal.env import (
    NEPTUNE_API_TOKEN,
    NEPTUNE_PROJECT,
)

__all__ = (
    "Context",
    "set_context",
    "set_project",
    "set_api_token",
    "global_context",
)


@dataclass
class Context:
    project: Optional[str] = None
    api_token: Optional[str] = None


_global_context: Optional[Context] = None
_lock = threading.RLock()


def set_project(project: str) -> Context:
    if not project:
        raise ValueError("Project name must be provided")

    with _lock:
        current_api_token = _global_context.api_token if _global_context else None
        return _set_global_context(Context(project=project, api_token=current_api_token))


def set_api_token(api_token: str) -> Context:
    if not api_token:
        raise ValueError("API token must be provided")

    with _lock:
        current_project = _global_context.project if _global_context else None
        return _set_global_context(Context(project=current_project, api_token=api_token))


def set_context(context: Optional[Context] = None) -> Context:
    if context is None:
        context = Context()  # Default to env-populated context
    return _set_global_context(context)


def global_context() -> Context:
    with _lock:
        if _global_context is None:
            _set_global_context(Context())

        return cast(Context, _global_context)


def _set_global_context(ctx: Context) -> Context:
    global _global_context

    project, api_token = ctx.project, ctx.api_token

    if project is None:
        try:
            project = NEPTUNE_PROJECT.get()
        except ValueError:
            raise ValueError(_PROJECT_NOT_SET_MESSAGE)

    if api_token is None:
        try:
            api_token = NEPTUNE_API_TOKEN.get()
        except ValueError:
            raise ValueError(_API_TOKEN_NOT_SET_MESSAGE)

    with _lock:
        _global_context = Context(project=project, api_token=api_token)
        return _global_context


_ERROR_TEMPLATE = """Unable to determine {thing}.

Set it using the environment variable {env_var} or by calling neptune.{func}().
"""

_PROJECT_NOT_SET_MESSAGE = _ERROR_TEMPLATE.format(
    thing="Neptune project name", env_var=NEPTUNE_PROJECT.name, func="set_project"
)
_API_TOKEN_NOT_SET_MESSAGE = _ERROR_TEMPLATE.format(
    thing="Neptune API token", env_var=NEPTUNE_API_TOKEN.name, func="set_api_token"
)
