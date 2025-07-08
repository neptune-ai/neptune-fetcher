import os
import pathlib
import tempfile
import urllib.parse
from datetime import (
    datetime,
    timedelta,
    timezone,
)

import azure.core.exceptions
import pytest

from neptune_query.internal.identifiers import AttributeDefinition
from neptune_query.internal.retrieval.attribute_values import fetch_attribute_values
from neptune_query.internal.retrieval.files import (
    SignedFile,
    download_file,
    download_file_retry,
    fetch_signed_urls,
)
from tests.e2e_query.conftest import extract_pages
from tests.e2e_query.data import PATH

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


@pytest.fixture
def file_path(client, project, experiment_identifier):
    project_identifier = project.project_identifier
    return extract_pages(
        fetch_attribute_values(
            client,
            project_identifier,
            [experiment_identifier],
            [AttributeDefinition(name=f"{PATH}/files/file-value.txt", type="file")],
        )
    )[0].value.path


def test_fetch_signed_url_missing(client, project, experiment_identifier):
    # when
    signed_urls = fetch_signed_urls(client, project.project_identifier, ["does-not-exist"], "read")

    # then
    assert len(signed_urls) == 1


def test_fetch_signed_url_single(client, project, experiment_identifier, file_path):
    # when
    signed_urls = fetch_signed_urls(client, project.project_identifier, [file_path], "read")

    # then
    assert len(signed_urls) == 1


def test_download_file_missing(client, project, experiment_identifier, temp_dir):
    # given
    signed_file = fetch_signed_urls(client, project.project_identifier, ["does-not-exist"], "read")[0]
    target_path = temp_dir / "test_download_file"

    # then
    with pytest.raises(azure.core.exceptions.ResourceNotFoundError):
        download_file(signed_url=signed_file.url, target_path=target_path)


def test_download_file_no_permission(client, project, experiment_identifier, file_path, temp_dir):
    # given
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"
    os.chmod(temp_dir, 0o000)  # No permission to write

    # then
    with pytest.raises(PermissionError):
        download_file(signed_url=signed_file.url, target_path=target_path)

    os.chmod(temp_dir, 0o755)  # Reset permissions


def test_download_file_single(client, project, experiment_identifier, file_path, temp_dir):
    # given
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"

    # when
    download_file(signed_url=signed_file.url, target_path=target_path)

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


def test_download_file_expired(client, project, experiment_identifier, file_path, temp_dir):
    # given
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"
    expired_url = _modify_signed_url(
        signed_file.url, se=[(datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")]
    )

    # then
    with pytest.raises(azure.core.exceptions.ClientAuthenticationError) as exc_info:
        download_file(signed_url=expired_url, target_path=target_path)

    assert "Signed expiry time" in str(exc_info.value)


def test_download_file_retry(client, project, experiment_identifier, file_path, temp_dir):
    # given
    project_identifier = project.project_identifier
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"
    expired_url = _modify_signed_url(
        signed_file.url, se=[(datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")]
    )
    expired_file = SignedFile(url=expired_url, path=signed_file.path)

    # when
    download_file_retry(
        client=client, project_identifier=project_identifier, signed_file=expired_file, target_path=target_path
    )

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


def test_download_file_no_retries(client, project, experiment_identifier, file_path, temp_dir):
    # given
    project_identifier = project.project_identifier
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"
    expired_url = _modify_signed_url(
        signed_file.url, se=[(datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")]
    )
    expired_file = SignedFile(url=expired_url, path=signed_file.path)

    # then
    with pytest.raises(azure.core.exceptions.ClientAuthenticationError):
        download_file_retry(
            client=client,
            project_identifier=project_identifier,
            signed_file=expired_file,
            target_path=target_path,
            retries=0,
        )


def test_download_file_retry_failed(client, project, experiment_identifier, file_path, temp_dir):
    # given
    project_identifier = project.project_identifier
    signed_file = fetch_signed_urls(client, project.project_identifier, [file_path], "read")[0]
    target_path = temp_dir / "test_download_file"
    invalid_file = SignedFile(url="https://invalid", path=signed_file.path)

    # then
    with pytest.raises(ValueError):
        download_file_retry(
            client=client, project_identifier=project_identifier, signed_file=invalid_file, target_path=target_path
        )


def _modify_signed_url(signed_url: str, **kwargs) -> str:
    original_url = urllib.parse.urlparse(signed_url)
    original_query = urllib.parse.parse_qs(original_url.query)
    original_query.update(kwargs)
    new_query = urllib.parse.urlencode(original_query, doseq=True)
    return original_url._replace(query=new_query).geturl()
