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

import functools
import json
import logging
import random
import time
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    TypeVar,
)

import httpx
import neptune_api.errors
from neptune_api.types import Response

from neptune_fetcher import exceptions
from neptune_fetcher.internal import env

logger = logging.getLogger(__name__)

T = TypeVar("T")


def handle_errors_default(func: Callable[..., Response[T]]) -> Callable[..., Response[T]]:
    return retry_backoff(
        max_tries=None,
        soft_max_time=env.NEPTUNE_FETCHER_RETRY_SOFT_TIMEOUT.get(),
        hard_max_time=env.NEPTUNE_FETCHER_RETRY_HARD_TIMEOUT.get(),
        backoff_strategy=exponential_backoff(jitter="full"),
    )(handle_api_errors(func))


def exponential_backoff(
    backoff_base: float = 0.5,
    backoff_factor: float = 2.0,
    backoff_max: float = 30.0,
    jitter: Optional[Literal["full", "equal"]] = None,
) -> Callable[[int], float]:
    def _calculate_sleep(tries: int) -> float:
        sleep = backoff_base * (backoff_factor ** (tries - 1))
        if jitter == "full":
            sleep *= random.uniform(0.0, 1.0)
        elif jitter == "equal":
            sleep *= random.uniform(0.5, 1.0)
        return min(sleep, backoff_max)

    return _calculate_sleep


def retry_backoff(
    max_tries: Optional[int] = None,
    soft_max_time: Optional[float] = None,
    hard_max_time: Optional[float] = None,
    backoff_strategy: Callable[[int], float] = exponential_backoff(),
) -> Callable[[Callable[..., Response[T]]], Callable[..., Response[T]]]:
    def decorator(func: Callable[..., Response[T]]) -> Callable[..., Response[T]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            total_tries = 0
            backoff_tries = 0
            start_time = time.monotonic()
            last_exc = None
            last_response = None
            rate_limit_time_extension = 0.0

            while True:
                response = None
                try:
                    response = func(*args, **kwargs)

                    if 200 <= response.status_code.value < 300:
                        return response
                except exceptions.NeptuneError:
                    raise  # give up immediately on NeptuneError
                except Exception as e:
                    last_exc = e

                if response is not None:
                    last_response = response

                total_tries += 1
                backoff_tries += 1
                if max_tries is not None and total_tries >= max_tries:
                    break

                if response is not None and "retry-after" in response.headers:
                    sleep_time = int(response.headers["retry-after"])
                    rate_limit_time_extension += sleep_time
                    backoff_tries = 0  # reset backoff tries counter when using a different strategy
                else:
                    sleep_time = backoff_strategy(backoff_tries)

                elapsed_time = time.monotonic() - start_time

                remaining_time = float("inf")
                if hard_max_time is not None:
                    remaining_time = min(remaining_time, hard_max_time - elapsed_time)
                if soft_max_time is not None:
                    remaining_time = min(remaining_time, soft_max_time + rate_limit_time_extension - elapsed_time)
                if remaining_time <= 0:
                    break
                sleep_time = min(remaining_time, sleep_time)
                time.sleep(sleep_time)

            # No more retries left
            elapsed_time = time.monotonic() - start_time
            if last_response:
                error = exceptions.NeptuneRetryError(
                    total_tries, elapsed_time, last_response.status_code.value, last_response.content
                )
            else:
                error = exceptions.NeptuneRetryError(total_tries, elapsed_time)
            if last_exc:
                raise error from last_exc
            else:
                raise error

        return wrapper

    return decorator


def handle_api_errors(func: Callable[..., Response[T]]) -> Callable[..., Response[T]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # In general, exceptions - subtypes of NeptuneError won't be retried - raising them makes the error final.
        # Other exceptions may be retried with a backoff - if persistent, they will eventually cause NeptuneRetryError.

        try:
            response = func(*args, **kwargs)

            _raise_for_error_code(response.status_code.value, response.content)
            return response
        except neptune_api.errors.ApiKeyRejectedError as e:
            # The API token is explicitly rejected by the backend -- don't retry anymore.
            raise exceptions.NeptuneInvalidCredentialsError() from e
        except neptune_api.errors.UnableToParseResponse as e:
            # Allow invalid 5xx errors to be retried - _raise_for_error_code will filter them out
            # reraise the original error (to retry it) otherwise
            if 500 <= e.response.status_code < 600:
                raise
            else:
                raise exceptions.NeptuneUnexpectedResponseError(
                    status_code=e.response.status_code,
                    content=e.response.content,
                )
        except httpx.TimeoutException as e:
            logger.warning(
                "Neptune API request timed out. Retrying...\n"
                "Check your network connection or increase the timeout by setting the "
                f"{env.NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS.name} environment variable "
                f"(currently: {env.NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS.get()} seconds)."
            )
            raise e
        except Exception as e:
            raise e

    return wrapper


def _raise_for_error_code(status_code: int, content: bytes) -> None:
    """
    Raise an exception for the given response status code and content if it indicates a permanent error.

    2xx status codes are considered successful responses and do not raise an exception.
    429, and 5xx status codes are considered transient and will be handled by the retry mechanism.
    3xx, and other 4xx status codes are unexpected responses and will raise an exception.

    If the content is a valid JSON and contains the "errorType" key, it is checked against known exceptions.
    If it is not the case, or the response does not match any known exceptions,
    `NeptuneUnexpectedResponseError` is raised.
    """
    if 200 <= status_code < 300 or status_code == 429 or 500 <= status_code < 600:
        return

    try:
        if content is not None:
            json_content = json.loads(content.decode("utf-8"))
            error_type = json_content.get("errorType")

            if error_type == "ACCESS_DENIED":
                raise exceptions.NeptuneProjectInaccessible()
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass

    raise exceptions.NeptuneUnexpectedResponseError(
        status_code=status_code,
        content=content,
    )
