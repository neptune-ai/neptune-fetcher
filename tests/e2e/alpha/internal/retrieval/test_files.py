import itertools as it
import os
import pathlib
import tempfile

import pytest

from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.attribute_values import fetch_attribute_values
from neptune_fetcher.alpha.internal.retrieval.files import (
    download_file,
    fetch_signed_urls,
)
from tests.e2e.alpha.internal.data import PATH

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


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
    )[0].value.path

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
    )[0].value.path
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"

    # when
    download_file(signed_url=signed_file.url, target_path=target_path)

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


def _extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))
