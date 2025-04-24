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
import pathlib
from dataclasses import dataclass
from typing import (
    Literal,
    Optional,
)

from azure.storage.blob import BlobClient
from neptune_api.client import AuthenticatedClient
from neptune_storage_api.api import storagebridge
from neptune_storage_api.models import (
    CreateSignedUrlsRequest,
    CreateSignedUrlsResponse,
    FileToSign,
    Permission,
)

from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
)
from neptune_fetcher.alpha.internal.retrieval import util


@dataclass(frozen=True)
class SignedFile:
    url: str
    path: str


def fetch_signed_urls(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    file_paths: list[str],
    permission: Literal["read", "write"] = "read",
) -> list[SignedFile]:
    body = CreateSignedUrlsRequest(
        files=[
            FileToSign(project_identifier=project_identifier, path=file_path, permission=Permission(permission))
            for file_path in file_paths
        ]
    )

    response = util.backoff_retry(storagebridge.signed_url.sync_detailed, client=client, body=body)

    data: CreateSignedUrlsResponse = response.parsed

    return [SignedFile(url=file_.url, path=file_.path) for file_ in data.files]


def download_file(
    signed_url: str,
    target_path: pathlib.Path,
    max_concurrency: int = env.NEPTUNE_FETCHER_FILES_MAX_CONCURRENCY.get(),
    timeout: Optional[int] = env.NEPTUNE_FETCHER_FILES_TIMEOUT.get(),
) -> pathlib.Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, mode="wb") as opened:
        blob_client = BlobClient.from_blob_url(signed_url)
        download_stream = blob_client.download_blob(max_concurrency=max_concurrency, timeout=timeout)
        for chunk in download_stream.chunks():
            opened.write(chunk)
    return target_path


def create_target_path(destination: pathlib.Path, experiment_name: str, attribute_path: str) -> pathlib.Path:
    relative_target_path = pathlib.Path(".") / experiment_name / attribute_path

    sanitized_parts = [_sanitize_path_part(part) for part in relative_target_path.parts]
    relative_target_path = pathlib.Path(*sanitized_parts)

    return destination / relative_target_path


def _sanitize_path_part(part: str, max_part_length: int = 255) -> str:
    # Replace invalid characters with underscores
    part = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in part)

    if len(part) > max_part_length:
        digest = hashlib.blake2b(part.encode("utf-8"), digest_size=8).hexdigest()
        part = f"{part[:max_part_length - len(digest) - 1]}_{digest}"

    return part
