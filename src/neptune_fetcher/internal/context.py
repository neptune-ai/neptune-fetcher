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
from typing import Optional

from .env import (
    NEPTUNE_API_TOKEN,
    NEPTUNE_PROJECT,
)

__all__ = (
    "Context",
    "get_context",
    "set_context",
    "set_project",
    "set_api_token",
    "validate_context",
)

from ..exceptions import (
    NeptuneApiTokenNotProvided,
    NeptuneProjectNotProvided,
)


@dataclass(frozen=True)
class Context:
    project: Optional[str] = None
    api_token: Optional[str] = None

    def with_project(self, project: str) -> "Context":
        """
        Copy the Context overwriting the project field.
        """
        if not project:
            raise ValueError("Project name must be provided")
        return Context(project=project, api_token=self.api_token)

    def with_api_token(self, api_token: str) -> "Context":
        """
        Copy the Context overwriting the api_token field.
        """
        if not api_token:
            raise ValueError("API token must be provided")
        return Context(project=self.project, api_token=api_token)


def set_project(project: str) -> Context:
    """
    Set the project in the context.
    Returns the set context.
    """
    global _context
    with _lock:
        _context = _context.with_project(project)
        return _context


def set_api_token(api_token: str) -> Context:
    """
    Set the API token in the context.
    Returns the set context.
    """
    global _context
    with _lock:
        _context = _context.with_api_token(api_token)
        return _context


def get_context() -> Context:
    """
    Return currently set global Context.
    """
    with _lock:
        return _context


def set_context(context: Optional[Context] = None) -> Context:
    """
    The context is automatically set from the environment variables (if they exist) on import of the module,
    but it's possible to override it with this function.

    If the argument is None, the global context is reset from environment variables.
    The following environment variables are used:
    - NEPTUNE_PROJECT
    - NEPTUNE_API_TOKEN

    Returns the set context.
    """
    global _context

    with _lock:
        _context = context or _context_from_env()
        return _context


def validate_context(context: Optional[Context] = None, validate_project: bool = False) -> Context:
    assert context is not None, "Context should have been set on import"

    if validate_project and context.project is None:
        raise NeptuneProjectNotProvided()
    if context.api_token is None:
        raise NeptuneApiTokenNotProvided()

    return context


def _context_from_env() -> Context:
    try:
        project = NEPTUNE_PROJECT.get()
    except ValueError:
        project = None

    try:
        api_token = NEPTUNE_API_TOKEN.get()
    except ValueError:
        api_token = None

    return Context(project=project, api_token=api_token)


_context: Context = _context_from_env()
_lock = threading.RLock()
