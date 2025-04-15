import itertools as it
import pathlib
import tempfile

import pytest

from neptune_fetcher.alpha.filters import Filter
from neptune_fetcher.alpha.internal.identifiers import RunIdentifier
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.attribute_values import fetch_attribute_values
from neptune_fetcher.alpha.internal.retrieval.files import (
    download_file,
    fetch_signed_urls,
)
from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

TEST_DATA_VERSION = "2025-04-14"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-retrieval-files-{TEST_DATA_VERSION}"
PATH = f"test/test-internal-retrieval-files-{TEST_DATA_VERSION}"


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

    run.assign_files({f"{PATH}/files/file-value.txt": b"Hello world!"})

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


def test_fetch_signed_url(client, project, experiment_identifier):
    # given
    project_identifier = project.project_identifier

    file_path = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(name=f"{PATH}/files/file-value.txt", type="file")],
        )
    )[0]

    # when
    signed_urls = fetch_signed_urls(client, project.project_identifier, [file_path], "read")

    # then
    assert len(signed_urls) == 1


def test_download_file(client, project, experiment_identifier, temp_dir):
    # given
    project_identifier = project.project_identifier

    file_path = _extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(name=f"{PATH}/files/file-value.txt", type="file")],
        )
    )[0]
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"

    # when
    download_file(signed_url=signed_file.url, target_path=target_path)

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Hello world!"


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
