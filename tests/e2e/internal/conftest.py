import itertools as it

import pytest

from neptune_fetcher.internal.context import set_project
from neptune_fetcher.internal.identifiers import RunIdentifier
from tests.e2e.data import TEST_DATA


@pytest.fixture(autouse=True)
def context(project):
    set_project(project.project_identifier)


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.internal.filters import Filter
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
