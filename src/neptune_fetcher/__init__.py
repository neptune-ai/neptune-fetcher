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
__all__ = [
    "ReadOnlyProject",
    "ReadOnlyRun",
]

from importlib.metadata import (
    PackageNotFoundError,
    version,
)

try:
    # This will raise PackageNotFoundError if the package is not installed
    version("neptune-experimental")

    raise ImportError(
        """You have `neptune-experimental` installed. The package is deprecated and causes errors.

Uninstall it before using `neptune-fetcher`:

    pip uninstall -y neptune-experimental neptune-fetcher; pip install neptune-fetcher

If you're using `uv`:

    uv pip uninstall neptune-experimental neptune-fetcher; uv pip install neptune-fetcher

    """
    )
except PackageNotFoundError:
    pass

from .read_only_project import ReadOnlyProject
from .read_only_run import ReadOnlyRun
