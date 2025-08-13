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
from __future__ import annotations

import contextlib
import dataclasses
import functools
import json
from dataclasses import dataclass
from importlib.metadata import (
    PackageNotFoundError,
    version,
)
from typing import (
    Callable,
    Generator,
    Optional,
    ParamSpec,
    TypeVar,
)

from neptune_api.types import Response

from neptune_query.internal.composition import concurrency

# This flag is used to control whether query metadata should be added to the request headers
# Can remove this after backend supports this properly (under ticket PY-171)
ADD_QUERY_METADATA = False


@functools.cache
def get_client_version() -> str:
    package_name = "neptune-query"
    try:
        package_version = version(package_name)
    except PackageNotFoundError:
        package_version = "unknown"
    return f"{package_name}/{package_version}"


@dataclass
class QueryMetadata:
    api_function: str
    client_version: str

    def __post_init__(self) -> None:
        self.api_function = self.api_function[:50]
        self.client_version = self.client_version[:50]


@contextlib.contextmanager
def use_query_metadata(api_function: str) -> Generator[None, None, None]:
    query_metadata = QueryMetadata(
        api_function=api_function,
        client_version=get_client_version(),
    )
    with concurrency.use_thread_local({"query_metadata": query_metadata}):
        yield


T = ParamSpec("T")
R = TypeVar("R")


def with_neptune_client_metadata(func: Callable[T, Response[R]]) -> Callable[T, Response[R]]:
    @functools.wraps(func)
    def wrapper(*args: T.args, **kwargs: T.kwargs) -> Response[R]:
        query_metadata: Optional[QueryMetadata] = concurrency.get_thread_local(
            "query_metadata", expected_type=QueryMetadata
        )
        if ADD_QUERY_METADATA and query_metadata:
            kwargs["x_neptune_client_metadata"] = json.dumps(dataclasses.asdict(query_metadata))
        return func(*args, **kwargs)

    return wrapper
