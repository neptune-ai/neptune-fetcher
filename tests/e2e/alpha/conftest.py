import hashlib
import os
import platform
import random
import uuid
from concurrent.futures import Executor
from datetime import (
    datetime,
    timezone,
)
from typing import Any

from _pytest.outcomes import Failed
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from pytest import fixture

import tests.e2e.alpha.generator as data
from neptune_fetcher.alpha import get_context
from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.api.api_client import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from tests.e2e.alpha.generator import ALL_STATIC_RUNS

API_TOKEN_ENV_NAME: str = "NEPTUNE_API_TOKEN"
NEPTUNE_E2E_REUSE_PROJECT = os.environ.get("NEPTUNE_E2E_REUSE_PROJECT", "False").lower() in {"true", "1"}
NEPTUNE_E2E_WORKSPACE = os.environ.get("NEPTUNE_E2E_WORKSPACE", "neptune-e2e")


@fixture(scope="session")
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
    return concurrency.create_thread_pool_executor()


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


@fixture(scope="session")
def new_project_id(client: AuthenticatedClient):
    project_name = generate_project_name(NEPTUNE_E2E_REUSE_PROJECT)
    if NEPTUNE_E2E_REUSE_PROJECT and project_exists(client, NEPTUNE_E2E_WORKSPACE, project_name):
        return f"{NEPTUNE_E2E_WORKSPACE}/{project_name}"

    create_project(client, project_name, NEPTUNE_E2E_WORKSPACE)

    project_id = f"{NEPTUNE_E2E_WORKSPACE}/{project_name}"
    data.log_runs(project_id, ALL_STATIC_RUNS)
    return project_id


@fixture(scope="session")
def new_project_context(new_project_id: str):
    return get_context().with_project(new_project_id)


def create_project(client, project_name, workspace):
    body = {"organizationIdentifier": workspace, "name": project_name, "visibility": "workspace"}
    args = {
        "method": "post",
        "url": "/api/backend/v1/projects",
        "json": body,
    }
    response = client.get_httpx_client().request(**args)
    response.raise_for_status()


def project_exists(client: AuthenticatedClient, workspace: str, project_id: str) -> bool:
    args = {
        "method": "get",
        "url": "/api/backend/v1/projects/get",
        "params": {"projectIdentifier": f"{workspace}/{project_id}"},
    }
    try:
        response = client.get_httpx_client().request(**args)
        response.raise_for_status()
        return True
    except Exception:  # Will catch 404 and other error responses
        return False


def generate_project_name(reuse: bool = False) -> str:
    if reuse:
        return f"pye2e-runs-{data_hash(ALL_STATIC_RUNS)}"
    return f"pye2e-runs-{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"


def data_hash(data: Any):
    # Generate unique hash for each day, computer and data
    node = platform.node()
    date = datetime.now().date().isoformat()
    return hashlib.md5((date + node + str(data)).encode()).hexdigest()
