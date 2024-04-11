#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
#
import base64
import json

import pytest
from mock import patch
from neptune.api.models import (
    FieldDefinition,
    FieldType,
    FloatField,
    IntField,
    LeaderboardEntry,
    StringField,
)
from neptune.internal.backends.api_model import Project
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.id_formats import (
    SysId,
    UniqueId,
)


@pytest.fixture(scope="session")
def api_token() -> str:
    return base64.b64encode(json.dumps({"api_address": ""}).encode()).decode()


class BackendMock:
    def __init__(self, *args, **kwargs):
        pass

    def get_project(self, project_id: UniqueId) -> Project:
        return Project(id=project_id, name="test_project", workspace="test_workspace", sys_id=SysId("PROJ-123"))

    def search_leaderboard_entries(self, *args, **kwargs):
        return iter(
            [
                LeaderboardEntry(
                    object_id="RUN-1",
                    fields=[
                        StringField(path="sys/id", value="RUN-1"),
                        StringField(path="sys/name", value="run1"),
                    ],
                ),
                LeaderboardEntry(
                    object_id="RUN-2",
                    fields=[
                        StringField(path="sys/id", value="RUN-2"),
                        StringField(path="sys/name", value="run2"),
                    ],
                ),
            ]
        )

    def get_fields_definitions(self, container_id, container_type, use_proto):
        return [
            FieldDefinition(path="sys/id", type=FieldType.STRING),
            FieldDefinition(path="sys/name", type=FieldType.STRING),
            FieldDefinition(path="sys/failed", type=FieldType.BOOL),
            FieldDefinition(path="metrics/string", type=FieldType.STRING),
            FieldDefinition(path="metrics/stringSeries", type=FieldType.STRING_SERIES),
            FieldDefinition(path="metrics/float", type=FieldType.FLOAT),
            FieldDefinition(path="metrics/int", type=FieldType.INT),
            FieldDefinition(path="metrics/floatSeries", type=FieldType.FLOAT_SERIES),
            FieldDefinition(path="metrics/datetime", type=FieldType.DATETIME),
            FieldDefinition(path="metrics/file", type=FieldType.FILE),
            FieldDefinition(path="metrics/fileSet", type=FieldType.FILE_SET),
            FieldDefinition(path="metrics/imageSeries", type=FieldType.IMAGE_SERIES),
            FieldDefinition(path="metrics/stringSet", type=FieldType.STRING_SET),
            FieldDefinition(path="metrics/gitRef", type=FieldType.GIT_REF),
            FieldDefinition(path="metrics/objectState", type=FieldType.OBJECT_STATE),
            FieldDefinition(path="metrics/notebookRef", type=FieldType.NOTEBOOK_REF),
            FieldDefinition(path="metrics/artifact", type=FieldType.ARTIFACT),
        ]

    def get_float_attribute(self, container_id, container_type, path):
        return FloatField(path="metrics/float", value=25.97)

    def get_int_attribute(self, container_id, container_type, path):
        return FloatField(path="metrics/int", value=97)

    def get_string_attribute(self, container_id, container_type, path):
        return StringField(path="metrics/string", value="Test string")

    def get_fields_with_paths_filter(self, container_id, container_type, paths):
        return [
            FloatField(path="metrics/float", value=25.97),
            IntField(path="metrics/int", value=97),
            StringField(path="metrics/string", value="Test string"),
        ]


@pytest.fixture(scope="session")
def hosted_backend() -> HostedNeptuneBackend:
    with patch("neptune_fetcher.read_only_project.HostedNeptuneBackend", BackendMock) as mock:
        yield mock
