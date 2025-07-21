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


@dataclass(frozen=True)
class Histogram:
    type: str
    edges: list[float]
    values: list[float]


@dataclass(frozen=True)
class File:
    label: str
    attribute_path: str
    step: Optional[float]
    path: str
    size_bytes: int
    mime_type: str

    def __repr__(self) -> str:
        return (
            f"File({self.label}, attribute_path={self.attribute_path}, "
            f"step={self.step}, size={_humanize_size(self.size_bytes)}, mime_type={self.mime_type})"
        )


def _humanize_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
