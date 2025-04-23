import os
import pathlib
import tempfile

import pytest

from neptune_fetcher.alpha.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal.composition.download_files import download_files
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.search import ContainerType
from tests.e2e.alpha.internal.data import (
    PATH,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = TEST_DATA.experiment_names[0]


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


def test_download_files_single(client, project, experiment_identifier, temp_dir):
    # when
    results = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value.txt"]),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert len(results) == 1
    assert results[0] == (
        experiment_identifier,
        AttributeDefinition(f"{PATH}/files/file-value.txt", "file"),
        temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt",
    )
    target_path = results[0][2]
    assert target_path.exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


def test_download_files_multiple(client, project, experiment_identifier, temp_dir):
    # when
    results = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value", f"{PATH}/files/file-value.txt"]),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert len(results) == 2
    assert set(results) == {
        (
            experiment_identifier,
            AttributeDefinition(f"{PATH}/files/file-value.txt", "file"),
            temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt",
        ),
        (
            experiment_identifier,
            AttributeDefinition(f"{PATH}/files/file-value", "file"),
            temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value",
        ),
    }

    for result in results:
        target_path = result[2]
        assert target_path.exists()
        with open(target_path, "rb") as file:
            content = file.read()
            if result[1].name == f"{PATH}/files/file-value":
                assert content == b"Binary content"
            else:
                assert content == b"Text content"
