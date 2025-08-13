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
    project_identifier: str
    experiment_name: Optional[str]
    run_id: Optional[str]
    attribute_path: str
    step: Optional[float]
    path: str
    size_bytes: int
    mime_type: str

    def __post_init__(self) -> None:
        if not (self.experiment_name or self.run_id):
            raise ValueError("Either 'experiment_name' or 'run_id' must be set for File.")
        if self.experiment_name and self.run_id:
            raise ValueError("Only one of 'experiment_name' or 'run_id' should be set for File.")

    def __repr__(self) -> str:
        return f"File(size={_humanize_size(self.size_bytes)}, mime_type={self.mime_type})"

    @property
    def container_identifier(self) -> str:
        """Returns a label for the container based on the file's experiment name or run ID."""
        if self.experiment_name:
            return self.experiment_name
        elif self.run_id:
            return self.run_id
        else:
            # This should never happen due to __post_init__ validation
            raise ValueError("File must have either an experiment name or a run ID.")


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
