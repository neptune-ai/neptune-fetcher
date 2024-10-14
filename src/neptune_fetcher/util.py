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
    Optional,
)

from neptune.api.fetching_series_values import PointValue
from neptune.internal.backends.utils import construct_progress_bar
from neptune.typing import ProgressBarType


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


def fetch_series_values(
    getter: Callable[..., Any], path: str, step_size: int = 10000, progress_bar: Optional[ProgressBarType] = None
) -> Iterator[PointValue]:
    first_batch = getter(limit=step_size)
    data_count = 0
    total = first_batch.total

    if total <= step_size:
        yield from first_batch.values
        return

    last_step_value = (first_batch.values[-1].step - 1) if first_batch.values else None
    with construct_progress_bar(progress_bar, f"Fetching {path} values") as bar:
        bar.update(by=data_count, total=total)

        while data_count < first_batch.total:
            batch = getter(from_step=last_step_value, limit=step_size)

            bar.update(by=len(batch.values), total=total)

            yield from batch.values

            last_step_value = batch.values[-1].step if batch.values else None
            data_count += len(batch.values)
