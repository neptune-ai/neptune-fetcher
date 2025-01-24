#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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

import os
import warnings
from typing import Any


def getenv_int(name: str, default: int, *, positive=True) -> int:
    value = os.environ.get(name)
    if value is None:
        return default

    try:
        value = int(value)
        if positive and value <= 0:
            raise ValueError
    except ValueError:
        raise ValueError(f"Environment variable {name} must be a positive integer, got '{value}'")

    return value


class NeptuneException(Exception):
    def __eq__(self, other: Any) -> bool:
        if type(other) is type(self):
            return super().__eq__(other) and str(self).__eq__(str(other))
        else:
            return False

    def __hash__(self) -> int:
        return hash((super().__hash__(), str(self)))


class NeptuneWarning(Warning):
    pass


warnings.simplefilter("once", category=NeptuneWarning)


def escape_nql_criterion(criterion):
    """
    Escape backslash and (double-)quotes in the string, to match what the NQL engine expects.
    """

    return criterion.replace("\\", r"\\").replace('"', r"\"")


# We keep a set of types we've warned the user about to make sure we warn about a type only once.
# This is necessary because of a bug in pandas, that causes duplicate warnings to be issued everytime after an
# DataFrame() is created (presumably only empty DF).
# The bug basically makes `warnings.simplefilter("once", NeptuneWarning)` not work as expected, and would flood
# the user with warnings in some cases.
_warned_types = set()


def warn_unsupported_value_type(type_: str) -> None:
    if type_ in _warned_types:
        return

    _warned_types.add(type_)
    warnings.warn(
        f"A value of type `{type_}` was returned by your query. This type is not supported by your installed version "
        "of neptune-fetcher. Values will evaluate to `None` and empty DataFrames. "
        "Upgrade neptune-fetcher to access this data.",
        NeptuneWarning,
    )
