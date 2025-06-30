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
from json import JSONDecodeError
from typing import (
    Any,
    Callable,
    TypeVar,
)
from google.protobuf.message import DecodeError as ProtobufDecodeError

import backoff
import httpx
from neptune_api.errors import (
    ApiKeyRejectedError,
    UnableToParseResponse, InvalidApiTokenException, UnableToDeserializeApiKeyError,
)
from neptune_api.types import Response

from neptune_fetcher import exceptions
from neptune_fetcher.exceptions import (
    NeptuneInvalidCredentialsError,
    NeptuneProjectInaccessible, NeptuneError,
)

logger = logging.getLogger(__name__)

A = TypeVar("A")
T = TypeVar("T")


def default_error_handling(func: Callable[..., Response[T]]) -> Callable[..., Response[T]]:
    decorators = [
        backoff.on_exception(
            backoff.expo,
            Exception,
            giveup=lambda e: isinstance(e, NeptuneError),
            max_tries=5,
            max_time=300,
            max_value=30,
        ),
        backoff.on_predicate(  # ideally merge into prev
            backoff.expo,
            predicate=lambda r: (r.status_code == 429 or 500 <= r.status_code < 600) and "x-rate-limit-retry-after-seconds" not in r.headers,
            max_tries=5,
            max_time=300,
            max_value=30,
        ),
        backoff.on_predicate(
            backoff.runtime,
            predicate=lambda r: (r.status_code == 429 or 500 <= r.status_code < 600) and "x-rate-limit-retry-after-seconds" in r.headers,
            value=lambda r: int(r.headers.get("x-rate-limit-retry-after-seconds")),
            jitter=None,
        ),
        with_api_errors_handling
    ]

    for decorator in reversed(decorators):
        func = decorator(func)
    return func


def with_api_errors_handling(func: Callable[..., Response[T]]) -> Callable[..., Response[T]]:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            response = func(*args, **kwargs)

            code = response.status_code.value
            if 0 <= code < 300:
                return response

            # Not a TooManyRequests or InternalServerError code
            if not (code == 429 or 500 <= code < 600):
                _raise_for_response(response.status_code, response.content)
            # if 429 or 500 <= code < 600, return - we will check the code again in downstream and retry
            return response
        except (InvalidApiTokenException, UnableToDeserializeApiKeyError, ApiKeyRejectedError) as e:
            # The API token is explicitly rejected by the backend -- don't retry anymore.
            raise NeptuneInvalidCredentialsError() from e
        except (JSONDecodeError, ProtobufDecodeError, UnableToParseResponse) as e:
            # Allow invalid response in 5xx errors to be retried, raise otherwise
            if e.response.status_code < 500:
                raise exceptions.NeptuneUnexpectedResponseError(
                    status_code=e.response.status_code,
                    content=e.response.content,
                ) from e
            raise e # retry
        except httpx.TimeoutException as e:
            logger.warning(
                "Neptune API request timed out. Retrying...\n"
                "Check your network connection or increase the timeout by setting the "
                "NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS environment variable (default: 60 seconds)."
            )
            raise e # retry
        except Exception as e:
            raise e # retry

    return wrapper


def _raise_for_response(status_code: int, content: bytes) -> Exception:
    """
    Raise an exception for the given response status code and content.

    The content is assumed to be valid JSON and contain the "errorType" key.
    If it is not the case, or the response does not match any known exceptions,
    `NeptuneUnexpectedResponseError` is raised.
    """
    try:
        if content is not None:
            json_content = json.loads(content.decode("utf-8"))

            error_type = json_content.get("errorType")
            if error_type == "ACCESS_DENIED":
                raise NeptuneProjectInaccessible()
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass

    raise exceptions.NeptuneUnexpectedResponseError(
        status_code=status_code,
        content=content,
    )
