import re
from unittest.mock import (
    Mock,
    call,
    patch,
)

import pytest
from neptune_api.errors import (
    ApiKeyRejectedError,
    UnableToParseResponse,
)
from pytest import fixture

from neptune_fetcher.exceptions import (
    NeptuneInvalidCredentialsError,
    NeptuneRetryError,
    NeptuneUnexpectedResponseError,
)
from neptune_fetcher.internal.retrieval.retry import (
    exponential_backoff,
    handle_api_errors,
    retry_backoff,
)


@fixture(autouse=True)
def sleep():
    with patch("time.sleep") as p:
        yield p


@fixture
def time():
    with patch("time.monotonic") as p:
        yield p


def response(code, content, headers=None):
    return Mock(status_code=Mock(value=code), content=content, headers=headers or {})


def response_200(content: bytes = b"OK"):
    return response(200, content=content)


def response_429(content: bytes = b"Error 429", retry_after: int = 1):
    return response(429, content=content, headers={"x-rate-limit-retry-after-seconds": str(retry_after)})


def response_500(content: bytes = b"Error 500"):
    return response(500, content=content)


@pytest.mark.parametrize(
    "retry_kwargs, side_effects, func_call_count, sleep_calls, exception, exception_message",
    [
        (dict(), [response_200()], 1, [], None, None),
        (
            dict(backoff_strategy=exponential_backoff(backoff_base=0.5)),
            [Exception, response_200()],
            2,
            [call(0.5)],
            None,
            None,
        ),
        (
            dict(backoff_strategy=exponential_backoff(0.5, 2, 10)),
            [Exception] * 7 + [response_200()],
            8,
            [call(n) for n in [0.5, 1, 2, 4, 8, 10, 10]],
            None,
            None,
        ),
        (
            dict(max_tries=5),
            [Exception] * 5,
            5,
            [call(n) for n in [0.5, 1, 2, 4]],
            NeptuneRetryError,
            "after 5 retries",
        ),
        (
            dict(max_tries=2),
            [response_500(), ValueError("foo")],
            2,
            [call(n) for n in [0.5]],
            NeptuneRetryError,
            re.compile("after 2 retries.*Last response status: 500.*Last response content: Error 500", re.DOTALL),
        ),
        (
            dict(max_tries=2),
            [response_500()] * 10,
            2,
            [call(n) for n in [0.5]],
            NeptuneRetryError,
            re.compile("after 2 retries.*Last response status: 500.*Last response content: Error 500", re.DOTALL),
        ),
        (
            dict(max_tries=2),
            [response_500(content=b'Error 500 {"header":{}, "content": ""}')] * 10,
            2,
            [call(0.5)],
            NeptuneRetryError,
            re.compile(
                "after 2 retries.*Last response status: 500"
                '.*Last response content: Error 500 {"header":{}, "content": ""}',
                re.DOTALL,
            ),
        ),
        (dict(max_tries=10), [ApiKeyRejectedError()], 1, [], NeptuneInvalidCredentialsError, "API token was rejected"),
        (
            dict(max_tries=3),
            [UnableToParseResponse(response=Mock(status_code=200, content=b"foo"), exception=Mock())],
            1,
            [],
            NeptuneUnexpectedResponseError,
            "unexpected response",
        ),
        (
            dict(max_tries=3),
            [UnableToParseResponse(response=Mock(status_code=500, content=b"foo"), exception=Mock())],
            3,
            [call(n) for n in [0.5, 1.0]],
            NeptuneRetryError,
            "after 3 retries",
        ),
        (
            dict(max_tries=3),
            [UnableToParseResponse(response=Mock(status_code=777, content=b"jackpot"), exception=Mock())],
            1,
            [],
            NeptuneUnexpectedResponseError,
            re.compile("unexpected response.*Response status: 777.*Response content: jackpot", re.DOTALL),
        ),
        (
            dict(),
            [response_429(retry_after=3), response_429(retry_after=1), response_429(retry_after=2), response_200()],
            4,
            [call(n) for n in [3.0, 1.0, 2.0]],
            None,
            None,
        ),
        (
            dict(),
            [response_500()] * 3 + [response_429(retry_after=8)] + [response_500()] * 4 + [response_200()],
            9,
            [call(n) for n in [0.5, 1.0, 2.0, 8.0, 0.5, 1.0, 2.0, 4.0]],
            None,
            None,
        ),
    ],
)
def test_retry_backoff(sleep, retry_kwargs, side_effects, func_call_count, sleep_calls, exception, exception_message):
    """`func` should be retried with the given arguments"""
    func = Mock(side_effect=side_effects)

    def decorator(f):
        return retry_backoff(**retry_kwargs)(handle_api_errors(f))

    if exception is not None:
        with pytest.raises(exception) as exc:
            decorator(func)(1, kw=2)
        if exception_message:
            exc.match(exception_message)
    else:
        result = decorator(func)()
        assert result == side_effects[-1]

    assert func.call_count == func_call_count
    if sleep_calls:
        assert sleep.call_count == len(sleep_calls)
        sleep.assert_has_calls(sleep_calls)
    else:
        sleep.assert_not_called()


def test_retry_passes_on_args(sleep):
    """`func` should be retried with the given arguments"""
    exp_response = response_200()
    func = Mock(side_effect=[Exception, Exception, exp_response])

    assert retry_backoff(max_tries=3)(func)(1, kw=2) == exp_response
    func.assert_has_calls([call(1, kw=2)] * 3)

    assert sleep.call_count == 2


def test_retry_limit_hit_on_exception_and_response_error_valid_cause(sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""
    exp_response = response_500()
    func = Mock(side_effect=[exp_response, ValueError("foo")])
    with pytest.raises(NeptuneRetryError) as exc:
        retry_backoff(max_tries=2)(func)()

    exc.match("after 2 retries")
    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")
    assert isinstance(exc.value.__cause__, ValueError)


def test_retry_timeout_soft(sleep, time):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""
    func = Mock(side_effect=[response_500()] * 5)
    time.side_effect = [0, 1, 2, 2, 3]
    with pytest.raises(NeptuneRetryError) as exc:
        retry_backoff(soft_max_time=2.0)(func)()

    exc.match("after 2 retries, 2.00 seconds")
    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")


def test_retry_timeout_hard(sleep, time):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""
    func = Mock(side_effect=[response_429(retry_after=3)] * 5)
    time.side_effect = [0, 3, 6, 6, 9]
    with pytest.raises(NeptuneRetryError) as exc:
        retry_backoff(soft_max_time=2.0, hard_max_time=5)(func)()

    exc.match("after 2 retries, 6.00 seconds")
    exc.match("Last response status: 429")
    exc.match("Last response content: Error 429")
