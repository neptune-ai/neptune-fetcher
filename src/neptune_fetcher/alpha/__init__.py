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

__all__ = [
    "Context",
    "get_context",
    "set_api_token",
    "set_context",
    "set_project",
    "list_experiments",
    "list_attributes",
    "fetch_experiments_table",
    "fetch_metrics",
]

from neptune_fetcher import (
    Context,
    fetch_experiments_table,
    fetch_metrics,
    get_context,
    list_attributes,
    list_experiments,
    set_api_token,
    set_context,
    set_project,
)
from neptune_fetcher.internal import warnings as _warnings

_warnings.warn_deprecated(
    "The `neptune_fetcher.alpha` package is deprecated and will be removed in future versions. "
    "Please use the `neptune_fetcher` package instead.",
)
