import itertools as it
import os
import time
from concurrent.futures import Executor
from datetime import timedelta

import pytest
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from neptune_scale import Run

from neptune_fetcher import ReadOnlyProject
from neptune_fetcher.alpha.filters import Filter
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.composition import concurrency
from neptune_fetcher.internal.context import set_project
from neptune_fetcher.internal.identifiers import RunIdentifier
from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs
from neptune_fetcher.util import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from tests.e2e.data import (
    NOW,
    PATH,
    TEST_DATA,
)

API_TOKEN_ENV_NAME: str = "NEPTUNE_API_TOKEN"


@pytest.fixture(scope="session")
def client() -> AuthenticatedClient:
    api_token = os.getenv(API_TOKEN_ENV_NAME)
    credentials = Credentials.from_api_key(api_key=api_token)
    config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=None)
    client = create_auth_api_client(
        credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=None
    )

    return client


@pytest.fixture(autouse=True)
def context(project):
    set_project(project.project_identifier)


@pytest.fixture(scope="module")
def executor() -> Executor:
    return concurrency.create_thread_pool_executor()


@pytest.fixture(scope="module")
def project(request):
    # Assume the project name and API token are set in the environment using the standard
    # NEPTUNE_PROJECT and NEPTUNE_API_TOKEN variables.
    #
    # Since ReadOnlyProject is essentially stateless, we can reuse the same
    # instance across all tests in a module.
    #
    # We also allow overriding the project name per module by setting the
    # module-level `NEPTUNE_PROJECT` variable.
    project_name = getattr(request.module, "NEPTUNE_PROJECT", None)
    return ReadOnlyProject(project=project_name)


@pytest.fixture(scope="module")
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
    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = Filter.name_in(TEST_DATA.experiment_names[0])
    experiment_attrs = extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


def extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
