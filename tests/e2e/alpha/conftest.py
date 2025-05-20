import hashlib
import itertools as it
import os
import platform
import random
import time
import uuid
from concurrent.futures import Executor
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import Any

import pytest
from _pytest.outcomes import Failed
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from neptune_scale import Run
from pytest import fixture

import tests.e2e.alpha.generator as data
from neptune_fetcher.alpha import (
    get_context,
    set_project,
)
from neptune_fetcher.alpha.filters import Filter
from neptune_fetcher.api.api_client import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.composition import concurrency
from neptune_fetcher.internal.identifiers import RunIdentifier
from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs
from tests.e2e.alpha.data import (
    NOW,
    PATH,
    TEST_DATA,
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


@pytest.fixture(autouse=True)
def context(project):
    set_project(project.project_identifier)


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(project, client):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_id),
                Filter.name_in(experiment.name),
            )
        )
        if existing.items:
            continue

        run = Run(
            project=project_id,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        run.log_configs(experiment.config)
        # This is how neptune-scale allows setting string set values currently
        run.log(tags_add=experiment.string_sets)

        for step in range(len(experiment.float_series[f"{PATH}/metrics/step"])):
            metrics_data = {path: values[step] for path, values in experiment.float_series.items()}
            metrics_data[f"{PATH}/metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

            series_data = {path: values[step] for path, values in experiment.string_series.items()}
            run.log_string_series(data=series_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        run.assign_files(experiment.files)

        runs[experiment.name] = run
    for run in runs.values():
        run.close()

    # Make sure all experiments are visible in the system before starting tests
    for _ in range(15):
        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project.project_identifier),
                Filter.name_in(*TEST_DATA.experiment_names),
            )
        )

        if len(existing.items) == len(TEST_DATA.experiment_names):
            return runs

        time.sleep(1)

    raise RuntimeError("Experiments did not appear in the system in time")


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.internal.filters import _Filter
    from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = _Filter.name_in(TEST_DATA.experiment_names[0])
    experiment_attrs = extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


def extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
