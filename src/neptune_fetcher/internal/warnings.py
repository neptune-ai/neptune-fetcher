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
# src/neptune_fetcher/alpha/internal/exception.py

import warnings

__all__ = [
    "NeptuneWarning",
    "warn_unsupported_value_type",
]


class NeptuneWarning(Warning):
    def __init__(self, message: str) -> None:
        super().__init__(message)


warnings.simplefilter("once", category=NeptuneWarning)

# We keep a set of types we've warned the user about to make sure we warn about a type only once.
# This is necessary because of a bug in pandas, that causes duplicate warnings to be issued everytime after an
# DataFrame() is created (presumably only empty DF).
# The bug basically makes `warnings.simplefilter("once", NeptuneWarning)` not work as expected, and would flood
# the user with warnings in some cases.
_warned_unsupported_types = set()


def warn_unsupported_value_type(type_: str) -> None:
    if type_ in _warned_unsupported_types:
        return

    _warned_unsupported_types.add(type_)
    warnings.warn(
        f"A value of type `{type_}` was returned by your query. This type is not supported by your installed version "
        "of neptune-fetcher. Values will evaluate to `None` and empty DataFrames. "
        "Upgrade neptune-fetcher to access this data.",
        NeptuneWarning,
        stacklevel=2,
    )


def warn_deprecated(message: str, stacklevel: int = 2) -> None:
    warnings.warn(
        message,
        NeptuneWarning,
        stacklevel=stacklevel,
    )
