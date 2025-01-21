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

import contextvars
import os
from typing import Optional

from ..context_data import (
    NEPTUNE_API_TOKEN,
    NEPTUNE_PROJECT,
    Context,
)

_neptune_context = contextvars.ContextVar("neptune_context")


def get_local_or_global_context(ctx: Optional[Context] = None) -> Context:
    return sanitize_context_falling_back_to_env(ctx or _neptune_context.get())


def set_global_context(ctx: Context):
    _neptune_context.set(ctx)


def sanitize_context_falling_back_to_env(ctx: Context) -> Context:
    if ctx.project and ctx.api_token:
        return ctx

    return Context(
        project=ctx.project or os.getenv(NEPTUNE_PROJECT, None),
        api_token=ctx.api_token or os.getenv(NEPTUNE_API_TOKEN, None),
    )
