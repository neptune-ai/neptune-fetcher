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
    Callable,
    Generator,
    Iterable,
    Optional,
    ParamSpec,
    TypeVar,
)

from neptune_query.internal.query_metadata_context import (
    QueryMetadata,
    get_query_metadata,
    use_query_metadata,
)

from .. import env

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
OUT = tuple[set[Future], Optional[R]]


def create_thread_pool_executor() -> Executor:
    max_workers = env.NEPTUNE_FETCHER_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def _use_query_metadata_context(
    query_metadata: Optional[QueryMetadata], downstream: Callable[P, OUT], *args: P.args, **kwargs: P.kwargs
) -> OUT:
    if not query_metadata:
        return downstream(*args, **kwargs)

    with use_query_metadata(query_metadata):
        return downstream(*args, **kwargs)


def generate_concurrently(
    items: Generator[T, None, None],
    executor: Executor,
    downstream: Callable[[T], OUT],
) -> OUT:
    query_metadata = get_query_metadata()
    try:
        head: T = next(items)
        futures = {
            executor.submit(_use_query_metadata_context, query_metadata, downstream, head),
            executor.submit(
                _use_query_metadata_context,
                query_metadata,
                lambda: generate_concurrently(
                    items,
                    executor,
                    downstream,
                ),
            ),
        }
        return futures, None
    except StopIteration:
        return set(), None


def fork_concurrently(executor: Executor, downstreams: Iterable[Callable[[], OUT]]) -> OUT:
    query_metadata = get_query_metadata()
    futures = {executor.submit(_use_query_metadata_context, query_metadata, downstream) for downstream in downstreams}
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
