import dataclasses
import os
import urllib.parse
from datetime import (
    datetime,
    timedelta,
    timezone,
)

import pytest

import neptune_query.exceptions
from neptune_query.internal.identifiers import AttributeDefinition
from neptune_query.internal.retrieval.attribute_values import fetch_attribute_values
from neptune_query.internal.retrieval.files import (
    download_file,
    download_file_complete,
    fetch_signed_urls,
)
from neptune_query.types import File
from tests.e2e_query.conftest import extract_pages
from tests.e2e_query.data import PATH

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


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


@pytest.fixture
def file_object_existing(client, project, file_path):
    return File(
        project_identifier=project.project_identifier,
        experiment_name="exp1",
        run_id=None,
        attribute_path=f"{PATH}/files/example-file",
        step=None,
        path=file_path,
        size_bytes=0,
        mime_type="application/octet-stream",
    )


@pytest.fixture
def file_object_non_existing(client, project):
    return File(
        project_identifier=project.project_identifier,
        experiment_name="exp1",
        run_id=None,
        attribute_path=f"{PATH}/files/does-not-exist",
        step=None,
        path="does-not-exist",
        size_bytes=0,
        mime_type="application/octet-stream",
    )


@pytest.mark.files
def test_fetch_signed_url_missing(client, project, file_object_non_existing):
    # when
    signed_urls = fetch_signed_urls(
        client,
        [file_object_non_existing],
        "read",
    )

    # then
    assert len(signed_urls) == 1
    assert signed_urls[0].path == "does-not-exist"


@pytest.mark.files
def test_fetch_signed_url_single(client, project, file_path, file_object_existing):
    # when
    signed_urls = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )

    # then
    assert len(signed_urls) == 1
    assert signed_urls[0].path == file_path


@pytest.mark.files
def test_download_file_missing(client, project, temp_dir, file_object_non_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_non_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"

    # when
    result = download_file(signed_file=signed_file, target_path=target_path)

    # then
    assert result.status == "not_found"


@pytest.mark.files
def test_download_file_no_permission(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"
    os.chmod(temp_dir, 0o000)  # No permission to write

    # then
    with pytest.raises(PermissionError):
        download_file(signed_file=signed_file, target_path=target_path)

    os.chmod(temp_dir, 0o755)  # Reset permissions


@pytest.mark.files
def test_download_file_single(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"

    # when
    download_file(signed_file=signed_file, target_path=target_path)

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_file_expired(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"
    expired_file = dataclasses.replace(signed_file, url=_expire_signed_url(signed_file.provider, signed_file.url))

    # when
    result = download_file(signed_file=expired_file, target_path=target_path)

    # then
    assert result.status == "expired"


@pytest.mark.files
def test_download_file_retry(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"
    expired_file = dataclasses.replace(signed_file, url=_expire_signed_url(signed_file.provider, signed_file.url))

    # when
    download_file_complete(client=client, signed_file=expired_file, target_path=target_path)

    # then
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_file_no_retries(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"
    expired_file = dataclasses.replace(signed_file, url=_expire_signed_url(signed_file.provider, signed_file.url))

    # then
    with pytest.raises(neptune_query.exceptions.NeptuneFileDownloadError):
        download_file_complete(
            client=client,
            signed_file=expired_file,
            target_path=target_path,
            max_tries=1,
        )


@pytest.mark.files
def test_download_file_retry_failed(client, project, file_path, temp_dir, file_object_existing):
    # given
    signed_file = fetch_signed_urls(
        client,
        [file_object_existing],
        "read",
    )[0]
    target_path = temp_dir / "test_download_file"
    invalid_file = dataclasses.replace(signed_file, url="https://invalid")

    # then
    with pytest.raises(neptune_query.exceptions.NeptuneFileDownloadError):
        download_file_complete(client=client, signed_file=invalid_file, target_path=target_path)


def _expire_signed_url(provider: str, signed_url: str) -> str:
    expired_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    if provider == "azure":
        return _modify_signed_url(signed_url, se=[expired_time.strftime("%Y-%m-%dT%H:%M:%SZ")])
    elif provider == "gcp":
        return _modify_signed_url(
            signed_url, **{"X-Goog-Date": [expired_time.strftime("%Y%m%dT%H%M%SZ")], "X-Goog-Expires": ["1"]}
        )
    else:
        raise ValueError(f"Unsupported provider: {provider}")


def _modify_signed_url(signed_url: str, **kwargs) -> str:
    original_url = urllib.parse.urlparse(signed_url)
    original_query = urllib.parse.parse_qs(original_url.query)
    original_query.update(kwargs)
    new_query = urllib.parse.urlencode(original_query, doseq=True)
    return original_url._replace(query=new_query).geturl()
