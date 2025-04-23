import itertools as it
import os
import pathlib
import tempfile

import pytest

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal.composition.download_files import download_files
from neptune_fetcher.alpha.internal.identifiers import RunIdentifier
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.search import (
    ContainerType,
    fetch_experiment_sys_attrs,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-04-19"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-composition-download-files-{TEST_DATA_VERSION}"
PATH = f"test/test-internal-composition-download-files-{TEST_DATA_VERSION}"


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.internal import identifiers

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            Filter.name_in(EXPERIMENT_NAME),
        )
    )
    if existing.items:
        return

    run_id = str(uuid.uuid4())

    run = Run(
        project=project_identifier,
        run_id=run_id,
        experiment_name=EXPERIMENT_NAME,
    )

    run.assign_files({f"{PATH}/files/file-value": b"Hello world!"})

    run.close()

    return run


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = Filter.name_in(EXPERIMENT_NAME)
    experiment_attrs = _extract_pages(
        fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter)
    )
    sys_id = experiment_attrs[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


def test_download_files(client, project, experiment_identifier, temp_dir):
    # when
    results = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=f"{PATH}/files/file-value"),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert len(results) == 1
    assert results[0] == (
        experiment_identifier,
        AttributeDefinition(f"{PATH}/files/file-value", "file"),
        temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value",
    )
    target_path = results[0][2]
    assert target_path.exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Hello world!"


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
