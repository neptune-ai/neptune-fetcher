#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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

import os
from typing import (
    Any,
    Callable,
    Iterator,
)

from neptune.api.fetching_series_values import PointValue


def getenv_int(name: str, default: int, *, positive=True) -> int:
    value = os.environ.get(name)
    if value is None:
        return default

    try:
        value = int(value)
        if positive and value <= 0:
            raise ValueError
    except ValueError:
        raise ValueError(f"Environment variable {name} must be a positive integer, got '{value}'")

    return value


def fetch_series_values(getter: Callable[..., Any], step_size: int = 10_000) -> Iterator[PointValue]:
    batch = getter(limit=step_size)
    yield from batch.values

    current_batch_size = len(batch.values)
    last_step_value = batch.values[-1].step if batch.values else None

    while current_batch_size == step_size:
        batch = getter(from_step=last_step_value, limit=step_size)

        yield from batch.values

        current_batch_size = len(batch.values)
        last_step_value = batch.values[-1].step if batch.values else None
