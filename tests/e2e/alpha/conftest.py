import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_api import AuthenticatedClient
from pytest import fixture

from neptune_fetcher.alpha.api_client import AuthenticatedClientBuilder


@fixture(scope="module")
def client() -> AuthenticatedClient:
    return AuthenticatedClientBuilder.build()


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"
