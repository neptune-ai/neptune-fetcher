#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
from __future__ import annotations

import contextlib
import functools
import threading
from dataclasses import dataclass
from typing import (
    Callable,
    Generator,
    Optional,
    ParamSpec,
    TypeVar,
)

from neptune_api.types import Response

_thread_local_storage = threading.local()


@dataclass
class QueryMetadata:
    api_method: str


@contextlib.contextmanager
def use_query_metadata(ctx: QueryMetadata) -> Generator[None, None, None]:
    _thread_local_storage.query_metadata = ctx
    try:
        yield
    finally:
        if hasattr(_thread_local_storage, "query_metadata"):
            del _thread_local_storage.query_metadata


def get_query_metadata() -> Optional[QueryMetadata]:
    return getattr(_thread_local_storage, "query_metadata", None)


T = ParamSpec("T")
R = TypeVar("R")


def with_neptune_client_metadata(func: Callable[T, Response[R]]) -> Callable[T, Response[R]]:
    """
    Decorator to add query metadata to the function.
    The metadata is stored in thread-local storage.
    """

    @functools.wraps(func)
    def wrapper(*args: T.args, **kwargs: T.kwargs) -> Response[R]:
        query_metadata = get_query_metadata()
        if query_metadata:
            kwargs["x_neptune_client_metadata"] = query_metadata
        return func(*args, **kwargs)

    return wrapper
