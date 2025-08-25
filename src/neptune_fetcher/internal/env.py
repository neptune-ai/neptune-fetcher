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

import os
from typing import (
    Callable,
    Generic,
    Optional,
    TypeVar,
)

__all__ = (
    "NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS",
    "NEPTUNE_API_TOKEN",
    "NEPTUNE_FETCHER_MAX_WORKERS",
    "NEPTUNE_PROJECT",
    "NEPTUNE_VERIFY_SSL",
    "NEPTUNE_FETCHER_RETRY_SOFT_TIMEOUT",
    "NEPTUNE_FETCHER_RETRY_HARD_TIMEOUT",
    "NEPTUNE_FETCHER_SYS_ATTRS_BATCH_SIZE",
    "NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE",
    "NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE",
    "NEPTUNE_FETCHER_SERIES_BATCH_SIZE",
    "NEPTUNE_FETCHER_QUERY_SIZE_LIMIT",
    "NEPTUNE_FETCHER_FILES_MAX_CONCURRENCY",
    "NEPTUNE_FETCHER_FILES_TIMEOUT",
    "NEPTUNE_ENABLE_COLORS",
)

T = TypeVar("T")


class EnvVariable(Generic[T]):
    _UNSET: T = object()  # type: ignore

    def __init__(self, name: str, value_mapper: Callable[[str], T], default_value: T = _UNSET):
        self.name = name
        self._value_mapper = value_mapper
        self._default_value = default_value

    def get(self) -> T:
        value = os.getenv(self.name)
        if value is None:
            if self._default_value is self._UNSET:
                raise ValueError(f"Environment variable {self.name} is not set")
            return self._default_value
        return self._map_value(value)

    def _map_value(self, value: str) -> T:
        return self._value_mapper(value)


def _map_str(value: str) -> str:
    return value.strip()


def _map_bool(value: str) -> bool:
    return value.lower() in {"true", "1"}


def _lift_optional(mapper: Callable[[str], T]) -> Callable[[str], Optional[T]]:
    def wrapped(value: str) -> Optional[T]:
        if not value:
            return None
        return mapper(value)

    return wrapped


NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS = EnvVariable[int]("NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS", int, 60)
NEPTUNE_API_TOKEN = EnvVariable[str]("NEPTUNE_API_TOKEN", _map_str)
NEPTUNE_PROJECT = EnvVariable[str]("NEPTUNE_PROJECT", _map_str)
NEPTUNE_VERIFY_SSL = EnvVariable[bool]("NEPTUNE_VERIFY_SSL", _map_bool, True)
NEPTUNE_FETCHER_RETRY_SOFT_TIMEOUT = EnvVariable[Optional[int]](
    "NEPTUNE_FETCHER_RETRY_SOFT_TIMEOUT", _lift_optional(int), 1800
)
NEPTUNE_FETCHER_RETRY_HARD_TIMEOUT = EnvVariable[Optional[int]](
    "NEPTUNE_FETCHER_RETRY_HARD_TIMEOUT", _lift_optional(int), 3600
)
NEPTUNE_FETCHER_MAX_WORKERS = EnvVariable[int]("NEPTUNE_FETCHER_MAX_WORKERS", int, 10)
NEPTUNE_FETCHER_SYS_ATTRS_BATCH_SIZE = EnvVariable[int]("NEPTUNE_FETCHER_EXPERIMENT_SYS_ATTRS_BATCH_SIZE", int, 10_000)
NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE = EnvVariable[int](
    "NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE", int, 10_000
)
NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE = EnvVariable[int](
    "NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE", int, 10_000
)
NEPTUNE_FETCHER_SERIES_BATCH_SIZE = EnvVariable[int]("NEPTUNE_FETCHER_SERIES_BATCH_SIZE", int, 10_000)
NEPTUNE_FETCHER_QUERY_SIZE_LIMIT = EnvVariable[int]("NEPTUNE_FETCHER_QUERY_SIZE_LIMIT", int, 220_000)
NEPTUNE_FETCHER_FILES_MAX_CONCURRENCY = EnvVariable[int]("NEPTUNE_FETCHER_FILES_MAX_CONCURRENCY", int, 1)
NEPTUNE_FETCHER_FILES_TIMEOUT = EnvVariable[Optional[int]]("NEPTUNE_FETCHER_FILES_TIMEOUT", _lift_optional(int), None)

NEPTUNE_ENABLE_COLORS = EnvVariable[bool]("NEPTUNE_ENABLE_COLORS", _map_bool, True)
