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
from dataclasses import dataclass
from typing import Literal

from neptune_api.client import AuthenticatedClient
from neptune_storage_api.api.storagebridge import signed_url
from neptune_storage_api.models import (
    CreateSignedUrlsRequest,
    CreateSignedUrlsResponse,
    FileToSign,
    Permission,
)

from neptune_fetcher.alpha.internal import identifiers
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

    response = util.backoff_retry(signed_url.sync_detailed, client=client, body=body)

    data: CreateSignedUrlsResponse = response.parsed

    return [SignedFile(url=file_.url, path=file_.path) for file_ in data.files]
