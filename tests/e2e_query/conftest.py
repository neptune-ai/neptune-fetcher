import itertools as it
import os
import time
from concurrent.futures import Executor
from dataclasses import dataclass
from datetime import timedelta

import pytest
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from neptune_scale import Run

from neptune_query.internal import identifiers
from neptune_query.internal.api_utils import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune_query.internal.composition import concurrency
from neptune_query.internal.context import set_project
from neptune_query.internal.filters import _Filter
from neptune_query.internal.identifiers import RunIdentifier
from neptune_query.internal.retrieval.search import fetch_experiment_sys_attrs
from tests.e2e_query.data import (
    FILE_SERIES_STEPS,
    NOW,
    PATH,
    TEST_DATA,
)

API_TOKEN_ENV_NAME: str = "NEPTUNE_API_TOKEN"


@dataclass
class Project:
    project_identifier: str


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
    # We also allow overriding the project name per module by setting the
    # module-level `NEPTUNE_PROJECT` variable.
    return Project(project_identifier=getattr(request.module, "NEPTUNE_PROJECT", None))


@pytest.fixture(scope="module")
def run_with_attributes(project, client):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_id),
                _Filter.name_eq(experiment.name),
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

            histogram_data = {path: values[step] for path, values in experiment.histogram_series.items()}
            run.log_histograms(histograms=histogram_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        run.log_configs(experiment.long_path_configs)
        run.log_string_series(data=experiment.long_path_series, step=1, timestamp=NOW)
        run.log_metrics(data=experiment.long_path_metrics, step=1, timestamp=NOW)

        run.assign_files(experiment.files)

        for step in range(FILE_SERIES_STEPS):
            file_data = {path: values[step] for path, values in experiment.file_series.items()}
            run.log_files(files=file_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        runs[experiment.name] = run
    for run in runs.values():
        run.close()

    # Make sure all experiments are visible in the system before starting tests
    for _ in range(15):
        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project.project_identifier),
                _Filter.any([_Filter.name_eq(experiment_name) for experiment_name in TEST_DATA.experiment_names]),
            )
        )

        if len(existing.items) == len(TEST_DATA.experiment_names):
            return runs

        time.sleep(1)

    raise RuntimeError("Experiments did not appear in the system in time")


@pytest.fixture(scope="module")
def experiment_identifiers(client, project, run_with_attributes) -> list[RunIdentifier]:
    from neptune_query.internal.filters import _Filter
    from neptune_query.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = _Filter.any(
        [_Filter.name_eq(experiment_name) for experiment_name in TEST_DATA.experiment_names]
    )
    experiment_attrs = extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )

    return [RunIdentifier(project_identifier, e.sys_id) for e in sorted(experiment_attrs, key=lambda e: e.sys_name)]


@pytest.fixture(scope="module")
def experiment_identifier(experiment_identifiers) -> RunIdentifier:
    return experiment_identifiers[0]


def extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
