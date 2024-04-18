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
from neptune_fetcher import ReadOnlyProject


def test__initialization(api_token, hosted_backend):
    # then
    ReadOnlyProject(project="test_project", api_token=api_token)


def test__fetch_runs_df(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_runs_df()

    # then
    assert results is not None
    assert sorted(results["sys/id"].values) == sorted(["RUN-1", "RUN-2"])


def test__fetch_runs(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_runs()

    # then
    assert results is not None
    assert sorted(results["sys/id"].values) == sorted(["RUN-1", "RUN-2"])
    assert sorted(results["sys/name"].values) == sorted(["run1", "run2"])


def test__list_runs(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = list(project.list_runs())

    # then
    assert results is not None
    assert sorted([result["sys/id"] for result in results]) == sorted(["RUN-1", "RUN-2"])
    assert sorted([result["sys/name"] for result in results]) == sorted(["run1", "run2"])


def test__fetch_read_only_runs(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = list(project.fetch_read_only_runs(with_ids=["RUN-1", "RUN-2"]))

    # then
    assert results is not None
    assert len(results) == 2
    assert results[0].with_id == "RUN-1"
    assert results[1].with_id == "RUN-2"
    assert results[0].project == project
    assert results[1].project == project
