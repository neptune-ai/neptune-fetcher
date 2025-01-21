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
import neptune_fetcher.alpha.internal.context as internal_context


def test_context_changing():
    ctx = npt.Context(project="test_project")
    assert internal_context.get_local_or_global_context(ctx).project == "test_project"

    ctx = npt.Context(api_token="test_token")
    assert internal_context.get_local_or_global_context(ctx).api_token == "test_token"

    ctx = npt.Context(project="test_project2", api_token="test_token2")
    assert internal_context.get_local_or_global_context(ctx).project == "test_project2"
    assert internal_context.get_local_or_global_context(ctx).api_token == "test_token2"


def test_context_itself():
    ctx = npt.Context(project="test_project")
    assert ctx.project == "test_project"

    ctx = npt.Context(api_token="test_token")
    assert ctx.api_token == "test_token"

    ctx = npt.Context(project="test_project2", api_token="test_token2")
    assert ctx.project == "test_project2"
    assert ctx.api_token == "test_token2"
