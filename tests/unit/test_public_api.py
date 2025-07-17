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
from __future__ import annotations

from typing import Generator
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from neptune_query import (
    fetch_experiments_table,
    fetch_metrics,
    fetch_series,
    list_attributes,
    list_experiments,
    runs,
)
from neptune_query.internal.query_metadata_context import QueryMetadata


@pytest.fixture(autouse=True, scope="module")
def _mock_dependencies() -> Generator[None]:
    with (
        patch(target="neptune_query.get_default_project_identifier", return_value="proj"),
        patch(target="neptune_query.internal.composition.list_containers.list_containers"),
        patch(target="neptune_query.internal.composition.list_attributes.list_attributes"),
        patch(target="neptune_query.internal.composition.fetch_metrics.fetch_metrics"),
        patch(target="neptune_query.internal.composition.fetch_table.fetch_table"),
        patch(target="neptune_query.internal.composition.fetch_series.fetch_series"),
        patch(target="neptune_query.runs.get_default_project_identifier", return_value="proj"),
        patch(target="neptune_query.runs._list_containers.list_containers"),
        patch(target="neptune_query.runs._list_attributes.list_attributes"),
        patch(target="neptune_query.runs._fetch_metrics.fetch_metrics"),
        patch(target="neptune_query.runs._fetch_table.fetch_table"),
        patch(target="neptune_query.runs._fetch_series.fetch_series"),
    ):
        yield


@patch("neptune_query.use_query_metadata")
def test_list_experiments_metadata(mock_use_query_metadata: MagicMock) -> None:
    list_experiments()
    _assert_metadata_set(mock_use_query_metadata, "list_experiments")


@patch("neptune_query.use_query_metadata")
def test_list_attributes_metadata(mock_use_query_metadata: MagicMock) -> None:
    list_attributes()
    _assert_metadata_set(mock_use_query_metadata, "list_attributes")


@patch("neptune_query.use_query_metadata")
def test_fetch_metrics_metadata(mock_use_query_metadata: MagicMock) -> None:
    fetch_metrics(experiments="exp", attributes="attr")
    _assert_metadata_set(mock_use_query_metadata, "fetch_metrics")


@patch("neptune_query.use_query_metadata")
def test_fetch_experiments_table_metadata(mock_use_query_metadata: MagicMock) -> None:
    fetch_experiments_table()
    _assert_metadata_set(mock_use_query_metadata, "fetch_experiments_table")


@patch("neptune_query.use_query_metadata")
def test_fetch_series_metadata(mock_use_query_metadata: MagicMock) -> None:
    fetch_series(experiments="exp", attributes="attr")
    _assert_metadata_set(mock_use_query_metadata, "fetch_series")


@patch("neptune_query.runs.use_query_metadata")
def test_list_runs_metadata(mock_use_query_metadata: MagicMock) -> None:
    runs.list_runs()
    _assert_metadata_set(mock_use_query_metadata, "runs.list_runs")


@patch("neptune_query.runs.use_query_metadata")
def test_list_runs_attributes_metadata(mock_use_query_metadata: MagicMock) -> None:
    runs.list_attributes()
    _assert_metadata_set(mock_use_query_metadata, "runs.list_attributes")


@patch("neptune_query.runs.use_query_metadata")
def test_fetch_runs_metrics_metadata(mock_use_query_metadata: MagicMock) -> None:
    runs.fetch_metrics(runs="run", attributes="attr")
    _assert_metadata_set(mock_use_query_metadata, "runs.fetch_metrics")


@patch("neptune_query.runs.use_query_metadata")
def test_fetch_runs_table_metadata(mock_use_query_metadata: MagicMock) -> None:
    runs.fetch_runs_table()
    _assert_metadata_set(mock_use_query_metadata, "runs.fetch_runs_table")


@patch("neptune_query.runs.use_query_metadata")
def test_fetch_runs_series_metadata(mock_use_query_metadata: MagicMock) -> None:
    runs.fetch_series(runs="run", attributes="attr")
    _assert_metadata_set(mock_use_query_metadata, "runs.fetch_series")


def _assert_metadata_set(mock_use_query_metadata: MagicMock, expected_func_name: str) -> None:
    mock_use_query_metadata.assert_called_once()
    call_args, _ = mock_use_query_metadata.call_args
    assert isinstance(call_args[0], QueryMetadata)
    assert call_args[0].api_function == expected_func_name
