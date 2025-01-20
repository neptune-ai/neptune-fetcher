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

import contextvars
import os
from typing import (
    Hashable,
    Optional,
)

neptune_api_token = contextvars.ContextVar("neptune_api_token")
neptune_project = contextvars.ContextVar("neptune_project")


def init_from_env():
    if (value := os.getenv("NEPTUNE_API_TOKEN")) is not None:
        neptune_api_token.set(value)

    if (value := os.getenv("NEPTUNE_PROJECT")) is not None:
        neptune_project.set(value)


class Context:
    project = None
    api_token = None

    def __init__(self, *, project: Optional[str] = None, api_token: Optional[str] = None):
        self.project = project
        self.api_token = api_token

    def get_project(self):
        return self.project

    def get_api_token(self):
        return self.api_token


def get_project(ctx: Optional[Context] = None):
    if ctx and (project := ctx.get_project()):
        return project
    return neptune_project.get()


def set_project(project: str):
    return neptune_project.set(project)


def _reset_project(token):
    neptune_project.reset(token)


def get_api_token(ctx: Optional[Context] = None):
    if ctx and (api_token := ctx.get_api_token()):
        return api_token
    return neptune_api_token.get()


def _set_api_token(api_token: str):
    return neptune_api_token.set(api_token)


def _reset_api_token(token):
    neptune_api_token.reset(token)


def _get_hash(ctx: Optional[Context] = None) -> Hashable:
    return hash(get_api_token(ctx))


init_from_env()
