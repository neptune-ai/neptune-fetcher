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

from __future__ import annotations

import concurrent
from concurrent.futures import (
    Executor,
    Future,
    ThreadPoolExecutor,
)
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    Optional,
    TypeVar,
)

from neptune_fetcher.internal import env

T = TypeVar("T")
R = TypeVar("R")
OUT = tuple[set[Future], Optional[R]]
_Params = dict[str, Any]


def create_thread_pool_executor() -> Executor:
    max_workers = env.NEPTUNE_FETCHER_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def generate_concurrently(
    items: Generator[T, None, None],
    executor: Executor,
    downstream: Callable[[T], OUT],
) -> OUT:
    try:
        head = next(items)
        futures = {
            executor.submit(downstream, head),
            executor.submit(generate_concurrently, items, executor, downstream),
        }
        return futures, None
    except StopIteration:
        return set(), None


def fork_concurrently(executor: Executor, downstreams: Iterable[Callable[[], OUT]]) -> OUT:
    futures = {executor.submit(downstream) for downstream in downstreams}
    return futures, None


def return_value(item: R) -> OUT:
    return set(), item


def gather_results(output: OUT) -> Generator[R, None, None]:
    futures, value = output
    if value is not None:
        yield value
    while futures:
        done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
        futures = not_done
        for future in done:
            new_futures, value = future.result()
            futures.update(new_futures)
            if value is not None:
                yield value
