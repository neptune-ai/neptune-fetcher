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

import time
from concurrent.futures import Executor
from typing import Generator

import pytest

from src.neptune_query.internal.composition.concurrency import (
    OUT,
    create_thread_pool_executor,
    fork_concurrently,
    gather_results,
    generate_concurrently,
    get_thread_local,
    return_value,
    use_thread_local,
)


@pytest.fixture(scope="module")
def executor() -> Generator[Executor, None, None]:
    with create_thread_pool_executor() as executor:
        yield executor


def test_return_value() -> None:
    # when
    futures, value = return_value(42)

    # then
    assert value == 42
    assert not futures


def test_gather_results(executor: Executor) -> None:
    # given
    def task() -> OUT:
        time.sleep(0.1)
        return return_value(42)

    # when
    results = gather_results(({executor.submit(task), executor.submit(task)}, None))

    # then
    assert list(results) == [42, 42]


def test_generate_concurrently(executor: Executor) -> None:
    # given
    def downstream(val: int) -> OUT:
        return return_value(val * 2)

    # when
    results = generate_concurrently(items=(i for i in range(5)), executor=executor, downstream=downstream)

    # then
    assert sorted(list(gather_results(results))) == [0, 2, 4, 6, 8]


def test_generate_concurrently_empty(executor: Executor) -> None:
    # given
    items = (i for i in range(0))  # empty generator

    def downstream(val: int) -> OUT:
        return return_value(val * 2)

    # when
    results = generate_concurrently(items, executor, downstream)

    # then
    assert list(gather_results(results)) == []


def test_fork_concurrently(executor: Executor) -> None:
    # given
    def task1() -> OUT:
        return return_value(1)

    def task2() -> OUT:
        return return_value(2)

    # when
    results = fork_concurrently(executor, [task1, task2])

    # then
    assert sorted(list(gather_results(results))) == [1, 2]


def test_thread_local_propagation(executor: Executor) -> None:
    # given
    key = "test_key"
    value = "test_value"

    # and
    def downstream() -> OUT:
        retrieved_value = get_thread_local(key, str)
        return return_value(retrieved_value)

    # when
    with use_thread_local({key: value}):
        # then
        assert get_thread_local(key, str) == value
        results = fork_concurrently(executor, [downstream])
        assert list(gather_results(results)) == [value]

    # and
    assert get_thread_local(key, str) is None


def test_get_thread_local_type_mismatch() -> None:
    # given
    key = "test_key"
    value = 123  # int, not str

    # when
    with use_thread_local({key: value}):
        # then
        with pytest.raises(RuntimeError, match="Expected <class 'str'> for key 'test_key', got <class 'int'>"):
            get_thread_local(key, str)


def test_complex_scenario(executor: Executor) -> None:
    # given
    def producer() -> Generator[int, None, None]:
        for i in range(3):
            yield i

    def downstream(val: int) -> OUT:
        local_val = get_thread_local("request_id", str)
        return return_value(f"{local_val}_{val}")

    def root_task() -> OUT:
        return generate_concurrently(producer(), executor, downstream)

    # when
    with use_thread_local({"request_id": "abc"}):
        results = fork_concurrently(executor, [root_task])
        # then
        assert sorted(list(gather_results(results))) == ["abc_0", "abc_1", "abc_2"]
