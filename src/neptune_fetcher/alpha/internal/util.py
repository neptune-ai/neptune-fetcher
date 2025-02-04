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
import time
from concurrent.futures import (
    Executor,
    Future,
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Optional,
    TypeVar,
)

from neptune_api import AuthenticatedClient
from neptune_retrieval_api.types import Response

from neptune_fetcher.alpha.internal import (
    attribute,
    env,
    identifiers,
)
from neptune_fetcher.util import NeptuneException

T = TypeVar("T")
R = TypeVar("R")
OUT = tuple[set[Future], Optional[R]]
_Params = dict[str, Any]


@dataclass
class Page(Generic[T]):
    items: list[T]


def fetch_pages(
    client: AuthenticatedClient,
    fetch_page: Callable[[AuthenticatedClient, _Params], R],
    process_page: Callable[[R], Page[T]],
    make_new_page_params: Callable[[_Params, Optional[R]], Optional[_Params]],
    params: _Params,
) -> Generator[Page[T], None, None]:
    page_params = make_new_page_params(params, None)
    while page_params is not None:
        data = fetch_page(client, page_params)
        page = process_page(data)
        page_params = make_new_page_params(page_params, data)

        yield page


def backoff_retry(
    func: Callable,
    *args: Any,
    max_tries: int = 5,
    backoff_factor: float = 0.5,
    max_backoff: float = 30.0,
    **kwargs: Any,
) -> Response[Any]:
    """
    Retries a function with exponential backoff. The function will be called at most `max_tries` times.

    :param func: The function to retry.
    :param max_tries: Maximum number of times `func` will be called, including retries.
    :param backoff_factor: Factor by which the backoff time increases.
    :param max_backoff: Maximum backoff time.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: The result of the function call.
    """

    if max_tries < 1:
        raise ValueError("max_tries must be greater than or equal to 1")

    tries = 0
    last_exc = None
    last_response = None

    while True:
        tries += 1
        try:
            response = func(*args, **kwargs)
        except Exception as e:
            response = None
            last_exc = e

        if response is not None:
            last_response = response

            code = response.status_code.value
            if 0 <= code < 300:
                return response

            # Not a TooManyRequests or InternalServerError code
            if not (code == 429 or 500 <= code < 600):
                raise NeptuneException(f"Unexpected server response {response.status_code}: {str(response.content)}")

        if tries == max_tries:
            break

        # A retryable error occurred, back off and try again
        backoff_time = min(backoff_factor * (2**tries), max_backoff)
        time.sleep(backoff_time)

    # No more retries left
    msg = []
    if last_exc:
        msg.append(f"Last exception: {str(last_exc)}")
    if last_response:
        msg.append(f"Last response: {last_response.status_code}: {str(last_response.content)}")
    if not msg:
        raise NeptuneException("Unknown error occurred when requesting data")

    raise NeptuneException(f"Failed to get response after {tries} retries. " + "\n".join(msg))


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


def fork_concurrently(item: T, executor: Executor, downstreams: Iterable[Callable[[T], OUT]]) -> OUT:
    futures = {executor.submit(downstream, item) for downstream in downstreams}
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


_EXPERIMENT_SIZE = 50


def _attribute_definition_size(attr: attribute.AttributeDefinition) -> int:
    return len(attr.name.encode("utf-8"))


def split_experiments(
    experiment_identifiers: list[identifiers.ExperimentIdentifier],
) -> Generator[list[identifiers.ExperimentIdentifier]]:
    """
    Splits a sequence of experiment identifiers into batches of size at most `NEPTUNE_FETCHER_QUERY_SIZE_LIMIT`.
    Use before fetching attribute definitions.
    """
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get()
    identifier_num_limit = max(query_size_limit // _EXPERIMENT_SIZE, 1)

    identifier_num = len(experiment_identifiers)
    batch_num = _ceil_div(identifier_num, identifier_num_limit)

    if batch_num <= 1:
        yield experiment_identifiers
    else:
        batch_size = _ceil_div(identifier_num, batch_num)
        for i in range(0, identifier_num, batch_size):
            yield experiment_identifiers[i : i + batch_size]


def split_experiments_attributes(
    experiment_identifiers: list[identifiers.ExperimentIdentifier],
    attribute_definitions: list[attribute.AttributeDefinition],
) -> Generator[tuple[list[identifiers.ExperimentIdentifier], list[attribute.AttributeDefinition]]]:
    """
    Splits a pair of experiment identifiers and attribute_definitions into batches that:
    When their length is added it is of size at most `NEPTUNE_FETCHER_QUERY_SIZE_LIMIT`.
    When their item count is multiplied, it is at most `NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE`.
    Use before fetching attribute values.
    """
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get()
    attribute_values_batch_size = env.NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.get()

    if not attribute_definitions:
        return

    attribute_batches = _split_attribute_definitions(attribute_definitions)
    max_attribute_batch_size = max(
        sum(_attribute_definition_size(attr) for attr in batch) for batch in attribute_batches
    )
    max_attribute_batch_len = max(len(batch) for batch in attribute_batches)

    experiments_batch: list[identifiers.ExperimentIdentifier] = []
    total_batch_size = max_attribute_batch_size
    for experiment in experiment_identifiers:
        if experiments_batch and (
            (len(experiments_batch) + 1) * max_attribute_batch_len > attribute_values_batch_size
            or total_batch_size + _EXPERIMENT_SIZE > query_size_limit
        ):
            for attribute_batch in attribute_batches:
                yield experiments_batch, attribute_batch
            experiments_batch = []
            total_batch_size = max_attribute_batch_size
        experiments_batch.append(experiment)
        total_batch_size += _EXPERIMENT_SIZE
    if experiments_batch:
        for attribute_batch in attribute_batches:
            yield experiments_batch, attribute_batch


def _split_attribute_definitions(
    attribute_definitions: list[attribute.AttributeDefinition],
) -> list[list[attribute.AttributeDefinition]]:
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get() - _EXPERIMENT_SIZE
    attribute_values_batch_size = env.NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.get()

    attribute_batches = []
    current_batch: list[attribute.AttributeDefinition] = []
    current_batch_size = 0
    for attr in attribute_definitions:
        attr_size = _attribute_definition_size(attr)
        if current_batch and (
            len(current_batch) >= attribute_values_batch_size or current_batch_size + attr_size > query_size_limit
        ):
            attribute_batches.append(current_batch)
            current_batch = []
            current_batch_size = 0
        current_batch.append(attr)
        current_batch_size += attr_size

    if current_batch:
        attribute_batches.append(current_batch)

    return attribute_batches


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b
