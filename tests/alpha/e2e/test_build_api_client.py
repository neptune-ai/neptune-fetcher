import pytest

import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.api_client import BuildApiClient


@pytest.fixture
def clear_cache_after_finishing_test():
    yield
    BuildApiClient.clear_cache()


def test_api_client_cache_works(clear_cache_after_finishing_test):
    client = BuildApiClient.build()
    client2 = BuildApiClient.build()
    assert client2 == client


def test_api_client_cache_works_for_separate_context(clear_cache_after_finishing_test):
    ctx = npt.Context(project="random_non_existing_test_project")
    client = BuildApiClient.build(ctx)
    client2 = BuildApiClient.build(ctx)
    assert client2 == client

    ctx2 = npt.Context(project="random_non_existing_test_project")
    client3 = BuildApiClient.build(ctx2)
    assert client3 == client

    # only api token matters, so different project are irrelevant with regard to ApiClient itself
    client_default = BuildApiClient.build()
    assert client_default == client
