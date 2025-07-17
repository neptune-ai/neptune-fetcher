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
from importlib.metadata import version
from typing import (
    Callable,
    Generator,
    ParamSpec,
    TypeVar,
)

from neptune_api.types import Response

from neptune_query.internal.composition import concurrency

package_name = "neptune-query"
package_version = version(package_name)


@dataclass
class QueryMetadata:
    api_function: str
    client_version: str = f"{package_name}/{package_version}"

    def __post_init__(self) -> None:
        self.api_function = self.api_function[:50]
        self.client_version = self.client_version[:50]


@contextlib.contextmanager
def use_query_metadata(query_metadata: QueryMetadata) -> Generator[None, None, None]:
    with concurrency.use_thread_local({"query_metadata": query_metadata}):
        yield


T = ParamSpec("T")
R = TypeVar("R")


def with_neptune_client_metadata(func: Callable[T, Response[R]]) -> Callable[T, Response[R]]:
    @functools.wraps(func)
    def wrapper(*args: T.args, **kwargs: T.kwargs) -> Response[R]:
        query_metadata: QueryMetadata = concurrency.get_thread_local("query_metadata", expected_type=QueryMetadata)
        if query_metadata:
            kwargs["x_neptune_client_metadata"] = json.dumps(dataclasses.asdict(query_metadata))
        return func(*args, **kwargs)

    return wrapper
