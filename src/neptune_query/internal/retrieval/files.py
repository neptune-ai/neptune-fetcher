#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import hashlib
import mimetypes
import pathlib
from dataclasses import dataclass
from typing import (
    Literal,
    Optional,
    Union,
)

import azure.core.exceptions
import requests
from azure.storage.blob import BlobClient as AzureBlobClient
from neptune_api.api.storage import signed_url_generic
from neptune_api.client import AuthenticatedClient
from neptune_api.models import (
    CreateSignedUrlsRequest,
    CreateSignedUrlsResponse,
    FileToSign,
    Permission,
    Provider,
)

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from ...exceptions import NeptuneFileDownloadError
from ...types import File
from .. import (
    env,
    identifiers,
)
from ..retrieval import retry


@dataclass(frozen=True)
class SignedFile:
    url: str
    path: str
    provider: Literal["azure", "gcp"]
    project_identifier: identifiers.ProjectIdentifier
    permission: Literal["read", "write"]


def fetch_signed_urls(
    client: AuthenticatedClient,
    files: list[File] | list[SignedFile],
    permission: Literal["read", "write"] = "read",
) -> list[SignedFile]:
    body = CreateSignedUrlsRequest(
        files=[
            FileToSign(project_identifier=file.project_identifier, path=file.path, permission=Permission(permission))
            for file in files
        ]
    )
    call_api = retry.handle_errors_default(with_neptune_client_metadata(signed_url_generic.sync_detailed))
    response = call_api(client=client, body=body)

    data: CreateSignedUrlsResponse = response.parsed
    if len(data.files) != len(files):
        missing_paths = {f.path for f in files} - {f.path for f in data.files}
        raise ValueError(
            f"Server returned {len(data.files)} / {len(files)} signed urls. " f"Missing paths: {missing_paths}"
        )
    return [
        SignedFile(
            url=file_.url,
            path=file_.path,
            provider=_verify_provider(file_.provider),
            project_identifier=identifiers.ProjectIdentifier(file_.project_identifier),
            permission=permission,
        )
        for file_ in data.files
    ]


def _verify_provider(provider: Provider) -> Literal["azure", "gcp"]:
    if provider == Provider.AZURE:
        return "azure"
    elif provider == Provider.GCP:
        return "gcp"
    else:
        raise ValueError(f"Unsupported provider: {provider}")


def refresh_signed_file(
    client: AuthenticatedClient,
    signed_file: SignedFile,
) -> SignedFile:
    """
    Refreshes the signed file URL by fetching a new signed URL from the server.
    This is useful when the original signed URL has expired or is no longer valid.
    """
    new_signed_files = fetch_signed_urls(client=client, files=[signed_file], permission=signed_file.permission)
    return new_signed_files[0]


@dataclass
class DownloadResult:
    status: Literal["success", "not_found", "expired", "transient"]
    status_code: Optional[int] = None
    content: Optional[Union[str, bytes]] = None


def download_file(
    signed_file: SignedFile,
    target_path: pathlib.Path,
    max_concurrency: int = env.NEPTUNE_QUERY_FILES_MAX_CONCURRENCY.get(),
    timeout: Optional[int] = env.NEPTUNE_QUERY_FILES_TIMEOUT.get(),
) -> DownloadResult:
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if signed_file.provider == "azure":
        result = _download_file_azure(signed_file, target_path, max_concurrency, timeout)
    elif signed_file.provider == "gcp":
        result = _download_file_requests(signed_file, target_path, timeout)
    else:
        raise ValueError(f"Unsupported provider: {signed_file.provider}")

    return result


def _download_file_azure(
    signed_file: SignedFile,
    target_path: pathlib.Path,
    max_concurrency: int = env.NEPTUNE_QUERY_FILES_MAX_CONCURRENCY.get(),
    timeout: Optional[int] = env.NEPTUNE_QUERY_FILES_TIMEOUT.get(),
) -> DownloadResult:
    try:
        blob_client = AzureBlobClient.from_blob_url(signed_file.url)
        download_stream = blob_client.download_blob(max_concurrency=max_concurrency, timeout=timeout)
        with open(target_path, mode="wb") as opened:
            for chunk in download_stream.chunks():
                opened.write(chunk)
        return DownloadResult(status="success")
    except azure.core.exceptions.ResourceNotFoundError as e:
        # This error indicates that the file does not exist in the storage - do not retry
        return DownloadResult(status="not_found", status_code=e.status_code, content=e.response.text())
    except azure.core.exceptions.ClientAuthenticationError as e:
        # This error indicates that the signed URL is no longer valid, likely due to expiration - try signing again
        return DownloadResult(status="expired", status_code=e.status_code, content=e.response.text())
    except azure.core.exceptions.HttpResponseError as e:
        # This error indicates a general HTTP error, which may be transient - retrying might help
        return DownloadResult(status="transient", status_code=e.status_code, content=e.response.text())


def _download_file_requests(
    signed_file: SignedFile,
    target_path: pathlib.Path,
    timeout: Optional[int] = env.NEPTUNE_QUERY_FILES_TIMEOUT.get(),
) -> DownloadResult:
    try:
        response = requests.get(signed_file.url, stream=True, timeout=timeout)
        response.raise_for_status()
        with open(target_path, mode="wb") as file:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                file.write(chunk)
        return DownloadResult(status="success")
    except requests.exceptions.HTTPError as e:
        try:
            if target_path.exists():
                target_path.unlink()
        except OSError:
            pass

        if e.response.status_code == 404:
            return DownloadResult(status="not_found", status_code=e.response.status_code, content=e.response.content)
        elif e.response.status_code == 400:
            return DownloadResult(status="expired", status_code=e.response.status_code, content=e.response.content)
        else:
            return DownloadResult(status="transient", status_code=e.response.status_code, content=e.response.content)


def download_file_complete(
    client: AuthenticatedClient,
    signed_file: SignedFile,
    target_path: pathlib.Path,
    max_tries: int = 3,
) -> Optional[pathlib.Path]:
    assert max_tries > 0, "max_tries must allow for at least one attempt"
    attempt = 1
    result = None
    try:
        while attempt <= max_tries:
            result = download_file(
                signed_file=signed_file,
                target_path=target_path,
            )
            if result.status == "success":
                return target_path
            elif result.status == "not_found":
                return None
            elif result.status == "expired":
                attempt += 1
                signed_file = refresh_signed_file(
                    client=client,
                    signed_file=signed_file,
                )
            elif result.status == "transient":
                attempt += 1
            else:
                raise ValueError(f"Unexpected download result status: {result.status}")
    except Exception as e:
        if result is not None:
            raise NeptuneFileDownloadError(last_status_code=result.status_code, last_content=result.content) from e
        else:
            raise NeptuneFileDownloadError() from e

    if result is not None:
        raise NeptuneFileDownloadError(
            details=f"Failed to download file after {max_tries} attempts.",
            last_status_code=result.status_code,
            last_content=result.content,
        )
    else:
        raise NeptuneFileDownloadError(details=f"Failed to download file after {max_tries} attempts.")


def create_target_path(destination: pathlib.Path, file: File) -> pathlib.Path:
    relative_target_path = pathlib.Path(".") / file.container_identifier / file.attribute_path
    if file.step is not None:
        relative_target_path = relative_target_path / f"step_{file.step:f}"
    extension = _guess_extension(file.mime_type)

    sanitized_parts = [_sanitize_path_part(part) for part in relative_target_path.parts]
    relative_target_path = pathlib.Path(*sanitized_parts)

    if extension:
        relative_target_path = relative_target_path.with_suffix(extension)

    return destination / relative_target_path


def _sanitize_path_part(part: str, max_part_length: int = 255) -> str:
    # Replace invalid characters with underscores
    part = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in part)

    if len(part) > max_part_length:
        digest = hashlib.blake2b(part.encode("utf-8"), digest_size=8).hexdigest()
        part = f"{part[:max_part_length - len(digest) - 1]}_{digest}"

    return part


def _guess_extension(mime_type: str) -> Optional[str]:
    return mimetypes.guess_extension(type=mime_type)
