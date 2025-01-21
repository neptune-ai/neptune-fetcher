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


__all__ = ["Context", "set_context", "set_project", "set_api_token"]


from .context_data import Context
from .internal.context import (
    sanitize_context_falling_back_to_env,
    set_global_context,
)


def init_from_env():
    env_context = Context()
    set_context(env_context)


def set_context(context: Context) -> Context:
    context = sanitize_context_falling_back_to_env(context)
    set_global_context(context)

    return context


def set_project(project: str):
    return set_context(Context(project=project))


def set_api_token(api_token: str):
    return set_context(Context(api_token=api_token))


init_from_env()
