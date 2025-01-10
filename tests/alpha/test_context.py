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
#

import neptune_fetcher.alpha as npt


def test_context_manager():
    with npt.Context(project="test_project"):
        assert npt.get_project() == "test_project"

    with npt.Context(api_token="test_token"):
        assert npt.get_api_token() == "test_token"

        with npt.Context(project="test_project2"):
            assert npt.get_project() == "test_project2"
            assert npt.get_api_token() == "test_token"

    with npt.Context(project="test_project3", api_token="test_token3"):
        assert npt.get_project() == "test_project3"
        assert npt.get_api_token() == "test_token3"

        with npt.Context(project="test_project4"):
            assert npt.get_project() == "test_project4"
            assert npt.get_api_token() == "test_token3"


def test_context_itself():
    ctx = npt.Context(project="test_project")
    assert ctx.get_project() == "test_project"

    ctx = npt.Context(api_token="test_token")
    assert ctx.get_api_token() == "test_token"

    ctx = npt.Context(project="test_project2", api_token="test_token2")
    assert ctx.get_project() == "test_project2"
    assert ctx.get_api_token() == "test_token2"
