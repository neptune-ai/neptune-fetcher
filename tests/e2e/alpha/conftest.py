import os
import random
import uuid
from concurrent.futures import Executor
from datetime import (
    datetime,
    timezone,
)

from _pytest.outcomes import Failed
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from pytest import fixture

from neptune_fetcher.alpha.internal import util
from neptune_fetcher.api.api_client import (
    create_auth_api_client,
    get_config_and_token_urls,
)

API_TOKEN_ENV_NAME: str = "NEPTUNE_API_TOKEN"


@fixture(scope="module")
def client() -> AuthenticatedClient:
    api_token = os.getenv(API_TOKEN_ENV_NAME)
    credentials = Credentials.from_api_key(api_key=api_token)
    config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=None)
    client = create_auth_api_client(
        credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=None
    )

    return client


@fixture(scope="module")
def executor() -> Executor:
    return util.create_thread_pool_executor()


def pytest_set_filtered_exceptions() -> list[type[BaseException]]:
    return [AssertionError, ValueError, Failed]


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"


def random_series(length=10, start_step=0):
    """Return a 2-tuple of step and value lists, both of length `length`"""
    assert length > 0
    assert start_step >= 0

    j = random.random()
    # Round to 0 to avoid floating point errors
    steps = [round((j + x) ** 2.0, 0) for x in range(start_step, length)]
    values = [round((j + x) ** 3.0, 0) for x in range(len(steps))]

    return steps, values
