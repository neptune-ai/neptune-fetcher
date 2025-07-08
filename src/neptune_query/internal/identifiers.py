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
from typing import NewType

ProjectIdentifier = NewType("ProjectIdentifier", str)  # e.g. "team/john.doe"
SysId = NewType("SysId", str)  # e.g. "KEY-1234"
SysName = NewType("SysName", str)  # e.g. "pye2e-fetcher-test-internal-attribute"
CustomRunId = NewType("CustomRunId", str)  # an uuid


@dataclass(frozen=True)
class RunIdentifier:
    project_identifier: ProjectIdentifier
    sys_id: SysId

    def __str__(self) -> str:
        return f"{self.project_identifier}/{self.sys_id}"


@dataclass(frozen=True)
class AttributeDefinition:
    name: str
    type: str


@dataclass(frozen=True)
class RunAttributeDefinition:
    run_identifier: RunIdentifier
    attribute_definition: AttributeDefinition
