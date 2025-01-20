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
