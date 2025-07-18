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
from typing import Optional

from neptune_query.internal import identifiers
from neptune_query.internal.retrieval.attribute_types import File


@dataclass(frozen=True)
class FileAttribute:
    label: str
    attribute_path: str
    step: Optional[float]


@dataclass(frozen=True)
class DownloadableFile:
    attribute: FileAttribute
    file: File

    @staticmethod
    def from_file(
        file: File, label: str, attribute_definition: identifiers.AttributeDefinition, step: Optional[float] = None
    ) -> "DownloadableFile":
        return DownloadableFile(
            attribute=FileAttribute(
                label=label,
                attribute_path=attribute_definition.name,
                step=step,
            ),
            file=file,
        )

    def __repr__(self) -> str:
        return (
            f"DownloadableFile({self.attribute.label}, attribute_path={self.attribute.attribute_path}, "
            f"step={self.attribute.step}, size={humanize_size(self.file.size_bytes)}, mime_type={self.file.mime_type})"
        )


def humanize_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
