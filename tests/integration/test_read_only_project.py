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


def test__fetch_runs_df__with_columns_regex(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_runs_df(columns=[], columns_regex="sys/.*")

    # then
    assert results is not None
    assert sorted(results.columns) == sorted(["sys/id", "sys/custom_run_id", "sys/failed"])


def test__fetch_runs_df__with_custom_id_regex(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_runs_df(custom_id_regex="alternative_*")

    # then
    assert results is not None
    assert results["sys/custom_run_id"].values == ["alternative_tesla"]


def test__fetch_runs(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_runs()

    # then
    assert results is not None
    assert sorted(results["sys/id"].values) == sorted(["RUN-1", "RUN-2"])


def test__list_runs(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = list(project.list_runs())

    # then
    assert results is not None
    assert sorted([result["sys/id"] for result in results]) == sorted(["RUN-1", "RUN-2"])


def test__fetch_read_only_runs__with_ids(api_token, hosted_backend):
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


def test__fetch_read_only_runs__custom_ids(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = list(project.fetch_read_only_runs(custom_ids=["alternative_tesla", "nostalgic_stallman"]))

    # then
    assert results is not None
    assert len(results) == 2
    assert results[0].with_id == "RUN-1"
    assert results[1].with_id == "RUN-2"
    assert results[0].project == project
    assert results[1].project == project


def test__list_experiments(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = list(project.list_experiments())

    # then
    assert results is not None
    assert sorted([result["sys/id"] for result in results]) == sorted(["EXP-1", "EXP-2"])


def test__fetch_experiments(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_experiments()

    # then
    assert results is not None
    assert sorted(results["sys/id"].values) == sorted(["EXP-1", "EXP-2"])


def test__fetch_experiments_df(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_experiments_df()

    # then
    assert results is not None
    assert sorted(results["sys/id"].values) == sorted(["EXP-1", "EXP-2"])


def test__fetch_experiments_df__with_custom_id_regex(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_experiments_df(custom_id_regex="custom_experiment_*")

    # then
    assert results is not None
    assert results["sys/custom_run_id"].values == ["custom_experiment_id"]


def test__fetch_experiments_df__with_columns_regex(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # when
    results = project.fetch_experiments_df(columns=[], columns_regex="sys/.*")

    # then
    assert results is not None
    assert sorted(results.columns) == sorted(["sys/id", "sys/custom_run_id", "sys/failed"])
