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

import json
from unittest.mock import Mock

import mock

from neptune_query.internal.composition.concurrency import get_thread_local
from neptune_query.internal.query_metadata_context import (
    QueryMetadata,
    use_query_metadata,
    with_neptune_client_metadata,
)


def test_query_metadata_truncation() -> None:
    # given
    long_string = "a" * 100
    metadata = QueryMetadata(api_function=long_string, client_version=long_string)

    # then
    assert len(metadata.api_function) == 50
    assert len(metadata.client_version) == 50
    assert metadata.api_function == "a" * 50
    assert metadata.client_version == "a" * 50


def test_use_query_metadata() -> None:
    # when
    assert get_thread_local("query_metadata", QueryMetadata) is None
    with use_query_metadata(api_function="test_func"):
        # then
        retrieved_metadata = get_thread_local("query_metadata", QueryMetadata)
        assert retrieved_metadata is not None
        assert retrieved_metadata.api_function == "test_func"

    # and
    assert get_thread_local("query_metadata", QueryMetadata) is None


def test_with_neptune_client_metadata_no_context() -> None:
    # given
    mock_api_call = Mock()

    # when
    decorated_call = with_neptune_client_metadata(mock_api_call)
    decorated_call(arg1="value1")

    # then
    mock_api_call.assert_called_once()
    _, kwargs = mock_api_call.call_args
    assert "x_neptune_client_metadata" not in kwargs


def test_with_neptune_client_metadata_with_context() -> None:
    # given
    mock_api_call = Mock()

    # when
    with (
        mock.patch("neptune_query.internal.query_metadata_context.ADD_QUERY_METADATA", True),
        mock.patch("neptune_query.internal.query_metadata_context.get_client_version", return_value="1.2.3"),
    ):
        decorated_call = with_neptune_client_metadata(mock_api_call)
        with use_query_metadata(api_function="test_api"):
            decorated_call(arg1="value1")

    # then
    mock_api_call.assert_called_once()
    _, kwargs = mock_api_call.call_args
    assert "x_neptune_client_metadata" in kwargs
    expected_json = json.dumps({"api_function": "test_api", "client_version": "1.2.3"})
    assert kwargs["x_neptune_client_metadata"] == expected_json
