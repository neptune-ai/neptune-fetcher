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
import contextlib
import threading
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
    ParamSpec,
    Type,
    TypeVar,
)

from .. import env

T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
OUT = tuple[set[Future[R]], Optional[R]]


def create_thread_pool_executor() -> Executor:
    max_workers = env.NEPTUNE_QUERY_MAX_WORKERS.get()
    return ThreadPoolExecutor(max_workers=max_workers)


def generate_concurrently(
    items: Generator[T, None, None],
    executor: Executor,
    downstream: Callable[[T], OUT],
) -> OUT:
    try:
        head: T = next(items)
        futures = {
            _submit_with_thread_local_propagation(executor, downstream, head),
            _submit_with_thread_local_propagation(executor, generate_concurrently, items, executor, downstream),
        }
        return futures, None
    except StopIteration:
        return set(), None


def fork_concurrently(executor: Executor, downstreams: Iterable[Callable[[], OUT]]) -> OUT:
    futures = {_submit_with_thread_local_propagation(executor, downstream) for downstream in downstreams}
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


_thread_local_storage = threading.local()


THREAD_LOCAL_PREFIX = "neptune_query_"


@contextlib.contextmanager
def use_thread_local(values: dict[str, Any]) -> Generator[None, None, None]:
    for key, value in values.items():
        setattr(_thread_local_storage, f"{THREAD_LOCAL_PREFIX}{key}", value)
    try:
        yield
    finally:
        for key in values.keys():
            attr = f"{THREAD_LOCAL_PREFIX}{key}"
            if hasattr(_thread_local_storage, attr):
                delattr(_thread_local_storage, attr)


def get_thread_local(key: str, expected_type: Type[T]) -> Optional[T]:
    value = getattr(_thread_local_storage, f"{THREAD_LOCAL_PREFIX}{key}", None)
    if value is not None and not isinstance(value, expected_type):
        raise RuntimeError(f"Expected {expected_type} for key '{key}', got {type(value)}")
    return value


def _submit_with_thread_local_propagation(
    executor: Executor, task: Callable[P, OUT], *args: P.args, **kwargs: P.kwargs
) -> Future[OUT]:
    thread_local_ctx = {
        key[len(THREAD_LOCAL_PREFIX) :]: getattr(_thread_local_storage, key)
        for key in dir(_thread_local_storage)
        if key.startswith(THREAD_LOCAL_PREFIX)
    }

    def _use_thread_local_context(downstream: Callable[P, OUT], *args: P.args, **kwargs: P.kwargs) -> OUT:
        with use_thread_local(thread_local_ctx):
            return downstream(*args, **kwargs)

    return executor.submit(_use_thread_local_context, task, *args, **kwargs)
