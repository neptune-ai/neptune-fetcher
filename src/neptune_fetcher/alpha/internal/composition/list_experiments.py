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
    Optional,
    Union,
)

from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha import context as _context
from neptune_fetcher.alpha.filters import (
    Attribute,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers as _identifiers
from neptune_fetcher.alpha.internal.api_client import client as _client
from neptune_fetcher.alpha.internal.api_client import search as _search
from neptune_fetcher.alpha.internal.composition import concurrency as _concurrency
from neptune_fetcher.alpha.internal.composition import type_inference as _infer

__all__ = ("list_experiments",)


def list_experiments(
    experiments: Optional[Union[str, Filter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
     Returns a list of experiment names in a project.

    `experiments` - a filter specifying which experiments to include
         - a regex that experiment name must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """

    validated_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(validated_context)
    project_identifier = _identifiers.ProjectIdentifier(validated_context.project)  # type: ignore

    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    with (
        _concurrency.create_thread_pool_executor() as executor,
        _concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        _infer.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            experiment_filter=experiments,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        pages = _search.fetch_experiment_sys_attrs(client, project_identifier, experiments)

        return list(exp.sys_name for page in pages for exp in page.items)
