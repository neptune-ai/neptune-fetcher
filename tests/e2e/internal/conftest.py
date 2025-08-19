import pytest


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse(run_with_attributes):
    pass
