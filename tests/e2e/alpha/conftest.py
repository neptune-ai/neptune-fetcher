import os
import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from pytest import fixture

from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.experiment import find_experiments
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
def experiment_name(client: AuthenticatedClient, run_init_kwargs) -> str:
    if "experiment_name" in run_init_kwargs:
        return run_init_kwargs["experiment_name"]

    run_id = run_init_kwargs["run_id"]
    project_id = run_init_kwargs["project"]
    experiment_filter = ExperimentFilter.eq(Attribute("sys/id", type="string"), run_id)
    experiment_names = find_experiments(client=client, project_id=project_id, experiment_filter=experiment_filter)
    return experiment_names[0]


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"
