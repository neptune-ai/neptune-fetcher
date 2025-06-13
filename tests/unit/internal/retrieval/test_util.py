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
from neptune_fetcher.internal.retrieval.util import backoff_retry


@fixture(autouse=True)
def sleep():
    with patch("time.sleep") as p:
        yield p


def response(code, content):
    return Mock(status_code=Mock(value=code), content=content)


@fixture
def response_200():
    return response(200, b"Hello")


@fixture
def response_500():
    return response(500, b"Error 500")


def test_retry_on_exception(response_200, sleep):
    """`func` should be retried with the given arguments"""
    func = Mock(side_effect=[Exception, Exception, response_200])

    assert backoff_retry(func, 1, kw=2, max_tries=3) == response_200
    func.assert_has_calls([call(1, kw=2)] * 3)

    assert sleep.call_count == 2


def test_retry_limit_hit_on_exception(sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[ValueError(f"error {x}") for x in range(1, 11)])
    with pytest.raises(NeptuneRetryError) as exc:
        backoff_retry(func, max_tries=7)

    assert func.call_count == 7
    assert sleep.call_count == func.call_count - 1

    exc.match("after 7 retries")


def test_retry_limit_hit_on_response_error(response_500, sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[response_500] * 10)
    with pytest.raises(NeptuneRetryError) as exc:
        backoff_retry(func, max_tries=5)

    exc.match("after 5 retries")
    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")


def test_retry_limit_hit_on_exception_and_response_error(response_500, sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[response_500, ValueError("foo")])
    with pytest.raises(NeptuneRetryError) as exc:
        backoff_retry(func, max_tries=2)

    exc.match("after 2 retries")
    exc.match("Last response status: 500")
    exc.match("Last response content: Error 500")
    assert isinstance(exc.value.__cause__, ValueError)


def test_retry_limit_hit_on_response_error_pattern(sleep):
    """`func` should be called `max_tries` times in total, then an exception should be raised"""

    func = Mock(side_effect=[response(500, b'Error 500 {"header":{}, "content": ""}')] * 10)
    with pytest.raises(NeptuneRetryError) as exc:
        backoff_retry(func, max_tries=5)

    exc.match("after 5 retries")
    exc.match("Last response status: 500")
    exc.match("""Last response content: Error 500 {"header":{}, "content": ""}""")


def test_dont_retry_on_api_token_rejected(sleep):
    """Should abort immediately on API token rejection with NeptuneInvalidCredentialsError"""

    func = Mock(side_effect=ApiKeyRejectedError)
    with pytest.raises(NeptuneInvalidCredentialsError) as exc:
        backoff_retry(func, max_tries=10)

    exc.match("API token was rejected")
    func.assert_called_once()
    sleep.assert_not_called()


def test_dont_retry_on_unable_to_parse_response_with_non_500_code(sleep):
    func = Mock(side_effect=UnableToParseResponse(response=Mock(status_code=200, content=b"foo"), exception=Mock()))
    with pytest.raises(NeptuneUnexpectedResponseError) as exc:
        backoff_retry(func, max_tries=10)

    exc.match("unexpected response")
    func.assert_called_once()
    sleep.assert_not_called()


def test_retry_on_unable_to_parse_response_with_code_500(sleep):
    func = Mock(side_effect=UnableToParseResponse(response=Mock(status_code=500, content=b"foo"), exception=Mock()))
    with pytest.raises(NeptuneRetryError) as exc:
        backoff_retry(func, max_tries=3)

    assert func.call_count == 3
    assert sleep.call_count == 2

    exc.match("after 3 retries")


def test_no_error(response_200, sleep):
    func = Mock(return_value=response_200)

    assert backoff_retry(func, 1, kw=2) == response_200
    func.assert_has_calls([call(1, kw=2)])
    sleep.assert_not_called()


def test_sleep_backoff(response_500, sleep):
    func = Mock(return_value=response_500)
    with pytest.raises(NeptuneRetryError):
        backoff_retry(func, max_tries=10, max_backoff=10)

    # Last call to sleep should be max backoff
    assert sleep.call_args.args[0] == 10
    # time.sleep() should be called with increasing values
    assert sleep.call_args_list == sorted(sleep.call_args_list)


def test_unexpected_server_response():
    """Should abort on unexpected response, and not retry"""
    func = Mock(return_value=response(777, b"jackpot!"))
    with pytest.raises(NeptuneUnexpectedResponseError) as exc:
        backoff_retry(func, max_tries=10)

    assert func.call_count == 1
    exc.match("jackpot!")
