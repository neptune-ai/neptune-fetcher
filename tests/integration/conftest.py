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
import datetime
import json

import pytest
from mock import patch
from neptune.api.models import (
    BoolField,
    DateTimeField,
    FieldDefinition,
    FieldType,
    FloatField,
    FloatPointValue,
    FloatSeriesField,
    FloatSeriesValues,
    IntField,
    LeaderboardEntry,
    NextPage,
    ObjectStateField,
    QueryFieldDefinitionsResult,
    QueryFieldsExperimentResult,
    QueryFieldsResult,
    StringField,
    StringSetField,
)
from neptune.internal.backends.api_model import (
    ApiExperiment,
    Project,
)
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.container_type import ContainerType
from neptune.internal.id_formats import (
    QualifiedName,
    SysId,
    UniqueId,
)


@pytest.fixture(scope="function")
def api_token() -> str:
    return base64.b64encode(json.dumps({"api_address": ""}).encode()).decode()


def create_leaderboard_entry(sys_id, custom_run_id, name, columns=None):
    return LeaderboardEntry(
        object_id=sys_id,
        fields=list(
            filter(
                lambda field: columns is None or field.path in columns,
                [
                    StringField(path="sys/id", value=sys_id),
                    StringField(path="sys/custom_run_id", value=custom_run_id),
                    StringField(path="sys/name", value=name),
                    BoolField(path="sys/failed", value=True),
                ],
            )
        ),
    )


class BackendMock:
    def __init__(self, *args, **kwargs):
        pass

    def get_project(self, project_id: UniqueId) -> Project:
        return Project(id=project_id, name="test_project", workspace="test_workspace", sys_id=SysId("PROJ-123"))

    def search_leaderboard_entries(self, columns, query, *args, **kwargs):
        print(query)

        output = []

        if str(query) != '((`sys/trashed`:bool = false) AND (`sys/id`:string = "RUN-1"))':
            output.append(create_leaderboard_entry("RUN-2", "nostalgic_stallman", "run2", columns))

        if (
            str(query) == "(`sys/trashed`:bool = false)"
            or str(query) == '((`sys/trashed`:bool = false) AND (`sys/id`:string = "RUN-1"))'
        ):
            output.append(
                create_leaderboard_entry("RUN-1", "alternative_tesla", "run1", columns),
            )

        return iter(output)

    def get_fields_definitions(self, *args, **kwargs):
        return [
            FieldDefinition(path="sys/id", type=FieldType.STRING),
            FieldDefinition(path="sys/custom_run_id", type=FieldType.STRING),
            FieldDefinition(path="sys/name", type=FieldType.STRING),
            FieldDefinition(path="sys/failed", type=FieldType.BOOL),
            FieldDefinition(path="metrics/string", type=FieldType.STRING),
            FieldDefinition(path="metrics/bool", type=FieldType.BOOL),
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

    def get_fields_with_paths_filter(self, *args, **kwargs):
        return [
            FloatField(path="metrics/float", value=25.97),
            IntField(path="metrics/int", value=97),
            StringField(path="metrics/string", value="Test string"),
            StringSetField(path="metrics/stringSet", values={"a", "b", "c"}),
            BoolField(path="metrics/bool", value=True),
            DateTimeField(path="metrics/datetime", value=datetime.datetime(2024, 1, 1, 12, 34, 56)),
            ObjectStateField(path="metrics/objectState", value="Inactive"),
            FloatSeriesField(path="metrics/floatSeries", last=25.97),
        ]

    def get_float_series_values(self, *args, **kwargs):
        return FloatSeriesValues(
            total=3,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56)),
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        )

    def query_fields_definitions_within_project(self, *args, **kwargs):
        return QueryFieldDefinitionsResult(
            entries=[
                FieldDefinition(path="sys/id", type=FieldType.STRING),
                FieldDefinition(path="sys/name", type=FieldType.STRING),
                FieldDefinition(path="sys/failed", type=FieldType.BOOL),
            ],
            next_page=NextPage(next_page_token=None, limit=None),
        )

    def query_fields_within_project(self, field_names_filter, *args, **kwargs):
        if field_names_filter == ["sys/name"]:
            return QueryFieldsResult(
                entries=[
                    QueryFieldsExperimentResult(
                        object_id="440ee146-442e-4d7c-a8ac-276ba940a071",
                        object_key="RUN-1",
                        fields=[
                            StringField(path="sys/name", value="powerful-sun-2"),
                        ],
                    ),
                    QueryFieldsExperimentResult(
                        object_id="2f24214f-c315-4c96-a82e-6d05aa017532",
                        object_key="RUN-2",
                        fields=[
                            StringField(path="sys/name", value="lazy-moon-2"),
                        ],
                    ),
                ],
                next_page=NextPage(next_page_token=None, limit=None),
            )
        else:
            return QueryFieldsResult(
                entries=[
                    QueryFieldsExperimentResult(
                        object_id="440ee146-442e-4d7c-a8ac-276ba940a071",
                        object_key="RUN-1",
                        fields=[
                            StringField(path="sys/custom_run_id", value="alternative_tesla"),
                        ],
                    ),
                    QueryFieldsExperimentResult(
                        object_id="2f24214f-c315-4c96-a82e-6d05aa017532",
                        object_key="RUN-2",
                        fields=[
                            StringField(path="sys/custom_run_id", value="nostalgic_stallman"),
                        ],
                    ),
                ],
                next_page=NextPage(next_page_token=None, limit=None),
            )

    def get_metadata_container(self, container_id, *args, **kwargs):
        if container_id == QualifiedName("CUSTOM/test_workspace/test_project/alternative_tesla"):
            internal_id = UniqueId("440ee146-442e-4d7c-a8ac-276ba940a071")
            sys_id = SysId("RUN-1")
        else:
            internal_id = UniqueId("2f24214f-c315-4c96-a82e-6d05aa017532")
            sys_id = SysId("RUN-2")

        return ApiExperiment(
            id=internal_id,
            type=ContainerType.RUN,
            sys_id=sys_id,
            workspace="test-workspace",
            project_name="test-project",
            trashed=False,
        )


@pytest.fixture(scope="function")
def hosted_backend() -> HostedNeptuneBackend:
    with patch("neptune_fetcher.read_only_project.HostedNeptuneBackend", return_value=BackendMock()) as mock:
        yield mock
