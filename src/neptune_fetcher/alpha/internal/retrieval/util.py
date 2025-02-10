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
    Generator,
    Generic,
    Optional,
    TypeVar,
)

from neptune_api import AuthenticatedClient
from neptune_retrieval_api.types import Response

from neptune_fetcher.alpha.exceptions import NeptuneError

T = TypeVar("T")
R = TypeVar("R")
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
                raise NeptuneError(
                    "Unexpected server response {status_code}: {content}",
                    status_code=response.status_code,
                    content=str(response.content),
                )

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
        raise NeptuneError("Unknown error occurred when requesting data")

    raise NeptuneError("Failed to get response after {tries} retries.\n{details}", tries=tries, details="\n".join(msg))
