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

import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
)

from neptune_retrieval_api.types import Response

from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.context import global_context
from neptune_fetcher.util import NeptuneException

T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    items: list[T]


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


def get_context(user_ctx: Optional[Context] = None) -> Context:
    """
    Return the global context, or the user-provided context if it is not None.
    If any of the fields in `user_ctx` are None, the global context will be used for that field.

    Always pass the user-provided context through this function, as it will ensure that all the fields are set.
    """

    global_ctx = global_context()
    if user_ctx is None:
        return global_ctx

    project = user_ctx.project
    api_token = user_ctx.api_token

    if project and api_token:
        return user_ctx

    if project is None:
        project = global_ctx.project

    if api_token is None:
        api_token = global_ctx.api_token

    return Context(project=project, api_token=api_token)
