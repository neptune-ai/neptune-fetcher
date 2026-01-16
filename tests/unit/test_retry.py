from unittest.mock import (
    Mock,
    call,
    patch,
)

import pytest
from pytest import fixture

from neptune_fetcher.generated.neptune_api.errors import ApiKeyRejectedError
from neptune_fetcher.internal.retrieval.retry import (
    exponential_backoff,
    handle_api_errors,
    retry_backoff,
)
from neptune_fetcher.util import (
    NeptuneException,
    rethrow_neptune_error,
)


@fixture(autouse=True)
def sleep():
    with patch("time.sleep") as p:
        yield p


def response(code, content):
    return Mock(status_code=Mock(value=code), content=content, headers={})


@fixture
def response_200():
    return response(200, b"Hello")


@fixture
def response_500():
    return response(500, b"Error 500")


def test_retry_on_exception(response_200, sleep):
    """`func` should be retried with the given arguments"""
    func = Mock(side_effect=[Exception, Exception, response_200])

    assert retry_backoff(max_tries=3)(func)(1, kw=2) == response_200
    func.assert_has_calls([call(1, kw=2)] * 3)

    assert sleep.call_count == 2


def test_retry_limit_hit_on_exception(sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[ValueError(f"error {x}") for x in range(1, 11)])
    with pytest.raises(NeptuneException) as exc:
        rethrow_neptune_error(retry_backoff(max_tries=7)(func))()

    assert func.call_count == 7
    assert sleep.call_count == func.call_count - 1

    exc.match("NeptuneRetryError: The Neptune server returned an error after 7 retries")


def test_retry_limit_hit_on_response_error(response_500, sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[response_500] * 10)
    with pytest.raises(NeptuneException) as exc:
        rethrow_neptune_error(retry_backoff(max_tries=5)(func))()

    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")


def test_retry_limit_hit_on_exception_and_response_error(response_500, sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[response_500, ValueError("foo")])
    with pytest.raises(NeptuneException) as exc:
        rethrow_neptune_error(retry_backoff(max_tries=2)(func))()

    assert isinstance(exc.value.__cause__, ValueError)
    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")


def test_dont_retry_on_api_token_rejected(sleep):
    """Should abort immediately on API token rejection"""

    func = Mock(side_effect=ApiKeyRejectedError)
    with pytest.raises(NeptuneException) as exc:
        rethrow_neptune_error(retry_backoff(max_tries=10)(handle_api_errors(func)))()

    exc.match("API token was rejected")
    func.assert_called_once()
    sleep.assert_not_called()


def test_no_error(response_200, sleep):
    func = Mock(return_value=response_200)

    assert retry_backoff()(func)(1, kw=2) == response_200
    func.assert_has_calls([call(1, kw=2)])
    sleep.assert_not_called()


def test_sleep_backoff(response_500, sleep):
    func = Mock(return_value=response_500)
    with pytest.raises(NeptuneException):
        rethrow_neptune_error(
            retry_backoff(max_tries=10, backoff_strategy=exponential_backoff(backoff_max=10, jitter=None))(func)
        )()

    # Last call to sleep should be max backoff
    assert sleep.call_args.args[0] == 10
    # time.sleep() should be called with increasing values
    assert sleep.call_args_list == sorted(sleep.call_args_list)


def test_unexpected_server_response():
    """Should abort on unexpected response, and not retry"""
    func = Mock(return_value=response(777, b"jackpot!"))
    with pytest.raises(NeptuneException) as exc:
        rethrow_neptune_error(retry_backoff(max_tries=10)(handle_api_errors(func)))()

    assert func.call_count == 1
    exc.match("jackpot!")
