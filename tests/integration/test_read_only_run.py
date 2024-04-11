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
from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)


def test__read_only_run__initialization(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # then
    ReadOnlyRun(read_only_project=project, with_id="RUN-1")


def test__read_only_run__field_names(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    field_names = list(run.field_names)

    # then
    assert sorted(field_names) == sorted(["sys/id", "sys/name", "metrics/int", "metrics/float", "metrics/string"])


def test__read_only_run__fetch__float__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/float"].fetch()

    # then
    assert val == 25.97


def test__read_only_run__fetch__float__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/float"])
    val = run["metrics/float"].fetch()

    # then
    assert val == 25.97


def test__read_only_run__fetch__int__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/int"].fetch()

    # then
    assert val == 97


def test__read_only_run__fetch__int__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/int"])
    val = run["metrics/int"].fetch()

    # then
    assert val == 97


def test__read_only_run__fetch__string__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/string"].fetch()

    # then
    assert val == "Test string"


def test__read_only_run__fetch__string__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/string"])
    val = run["metrics/string"].fetch()

    # then
    assert val == "Test string"
