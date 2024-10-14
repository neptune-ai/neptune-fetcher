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
import datetime

from mock import call
from neptune.api.models import (
    FloatPointValue,
    FloatSeriesValues,
)
from neptune.internal.container_type import ContainerType

from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)


def test__initialization(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # then
    ReadOnlyRun(read_only_project=project, with_id="RUN-1")


def test__field_names(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    field_names = list(run.field_names)

    # then
    assert sorted(field_names) == sorted(
        [
            "metrics/bool",
            "metrics/datetime",
            "metrics/float",
            "metrics/floatSeries",
            "metrics/int",
            "metrics/objectState",
            "metrics/string",
            "metrics/stringSet",
            "sys/failed",
            "sys/id",
            "sys/custom_run_id",
            "sys/name",
        ]
    )


def test__fetch__float__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/float"].fetch()

    # then
    assert val == 25.97


def test__fetch__float__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/float"])
    val = run["metrics/float"].fetch()

    # then
    assert val == 25.97


def test__fetch__int__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/int"].fetch()

    # then
    assert val == 97


def test__fetch__int__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/int"])
    val = run["metrics/int"].fetch()

    # then
    assert val == 97


def test__fetch__string__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/string"].fetch()

    # then
    assert val == "Test string"


def test__fetch__string__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/string"])
    val = run["metrics/string"].fetch()

    # then
    assert val == "Test string"


def test__fetch__bool__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/bool"].fetch()

    # then
    assert val is True


def test__fetch__bool__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/bool"])
    val = run["metrics/bool"].fetch()

    # then
    assert val is True


def test__fetch__datetime__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/datetime"].fetch()

    # then
    assert val == datetime.datetime(2024, 1, 1, 12, 34, 56)


def test__fetch__datetime__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/datetime"])
    val = run["metrics/datetime"].fetch()

    # then
    assert val == datetime.datetime(2024, 1, 1, 12, 34, 56)


def test__fetch__object_state__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/objectState"].fetch()

    # then
    assert val == "Inactive"


def test__fetch__object_state__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/objectState"])
    val = run["metrics/objectState"].fetch()

    # then
    assert val == "Inactive"


def test__fetch__string_set__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/stringSet"].fetch()

    # then
    assert val == {"a", "b", "c"}


def test__fetch__string_set__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/stringSet"])
    val = run["metrics/stringSet"].fetch()

    # then
    assert val == {"a", "b", "c"}


def test__fetch__float_series__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    val = run["metrics/floatSeries"].fetch()

    # then
    assert val == 25.97


def test__fetch__float_series__prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch(["metrics/floatSeries"])
    val = run["metrics/floatSeries"].fetch()

    # then
    assert val == 25.97


def test__fetch_values__float_series__without_prefetch(api_token, hosted_backend):
    # given
    project = ReadOnlyProject(project="test_project", api_token=api_token)

    # and
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    df = run["metrics/floatSeries"].fetch_values()

    # then
    assert df["value"].to_list() == [1.0, 2.0, 3.0]
    assert df["step"].to_list() == [1, 2, 3]


def test__fetch_values__float_series__prefetch(api_token, hosted_backend):
    # given
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=3,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56)),
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]

    # and
    project = ReadOnlyProject(project="test_project", api_token=api_token)
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch_series_values(["metrics/floatSeries"])

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=True,
                from_step=None,
                limit=10000,
            ),
        ]
    )

    # when
    hosted_backend.get_float_series_values.reset_mock()
    df = run["metrics/floatSeries"].fetch_values()

    # then
    assert not hosted_backend.get_float_series_values.called

    # then
    assert df["value"].to_list() == [1.0, 2.0, 3.0]
    assert df["step"].to_list() == [1, 2, 3]


def test__fetch_values__float_series__no_inherited(api_token, hosted_backend):
    # given
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=2,
            values=[
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]

    # and
    project = ReadOnlyProject(project="test_project", api_token=api_token)
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    df = run["metrics/floatSeries"].fetch_values(include_inherited=False)

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=False,
                from_step=None,
                limit=10000,
            ),
        ]
    )
    assert df["value"].to_list() == [2.0, 3.0]
    assert df["step"].to_list() == [2, 3]


def test__fetch_values__float_series__prefetch__no_inherited(api_token, hosted_backend):
    # given
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=2,
            values=[
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]

    # and
    project = ReadOnlyProject(project="test_project", api_token=api_token)
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch_series_values(["metrics/floatSeries"], include_inherited=False)

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=False,
                from_step=None,
                limit=10000,
            ),
        ]
    )

    # when
    hosted_backend.get_float_series_values.reset_mock()
    df = run["metrics/floatSeries"].fetch_values(include_inherited=False)

    # then
    assert not hosted_backend.get_float_series_values.called
    assert df["value"].to_list() == [2.0, 3.0]
    assert df["step"].to_list() == [2, 3]


def test__fetch_values__float_series__prefetch__different_inheritance(api_token, hosted_backend):
    # given
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=2,
            values=[
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]

    # and
    project = ReadOnlyProject(project="test_project", api_token=api_token)
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch_series_values(["metrics/floatSeries"], include_inherited=False)

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=False,
                from_step=None,
                limit=10000,
            ),
        ]
    )

    # when
    hosted_backend.get_float_series_values.reset_mock()
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=3,
            values=[
                FloatPointValue(step=1, value=1.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56)),
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]
    df = run["metrics/floatSeries"].fetch_values(include_inherited=True)

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=True,
                from_step=None,
                limit=10000,
            ),
        ]
    )
    assert df["value"].to_list() == [1.0, 2.0, 3.0]
    assert df["step"].to_list() == [1, 2, 3]

def test__fetch_values__float_series__prefetch__with_step_range(api_token, hosted_backend):
    # given
    hosted_backend.get_float_series_values.side_effect = [
        FloatSeriesValues(
            total=2,
            values=[
                FloatPointValue(step=2, value=2.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 57)),
                FloatPointValue(step=3, value=3.0, timestamp=datetime.datetime(2024, 1, 1, 12, 34, 58)),
            ],
        ),
    ]

    # and
    project = ReadOnlyProject(project="test_project", api_token=api_token)
    run = ReadOnlyRun(read_only_project=project, with_id="RUN-1")

    # when
    run.prefetch_series_values(["metrics/floatSeries"], step_range=(2., None))

    # then
    hosted_backend.get_float_series_values.assert_has_calls(
        [
            call(
                container_id="test_workspace/test_project/RUN-1",
                container_type=ContainerType.RUN,
                path=["metrics", "floatSeries"],
                include_inherited=True,
                from_step=2.,
                limit=10000,
            )
        ]
    )

    # when
    hosted_backend.get_float_series_values.reset_mock()
    df = run["metrics/floatSeries"].fetch_values(step_range=(2., None))

    # then
    assert not hosted_backend.get_float_series_values.called
    assert df["value"].to_list() == [2.0, 3.0]
    assert df["step"].to_list() == [2, 3]