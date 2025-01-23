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

from typing import (
    List,
    Optional,
    Union,
)

from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.api_client import AuthenticatedClientBuilder
from neptune_fetcher.alpha.filter import ExperimentFilter
from neptune_fetcher.alpha.internal.context import get_local_or_global_context
from neptune_fetcher.alpha.internal.experiment import find_experiments


def list_experiments(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    context: Optional[Context] = None,
) -> List[str]:
    """
     Returns a list of experiment names in a project.

    `experiments` - a filter specifying which experiments to include
         - a regex that experiment name must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """
    client = AuthenticatedClientBuilder.build(context=context)
    project = get_local_or_global_context(context).project
    assert project is not None, "Project must be set in the context"

    return find_experiments(client, project, experiment_filter=experiments)
