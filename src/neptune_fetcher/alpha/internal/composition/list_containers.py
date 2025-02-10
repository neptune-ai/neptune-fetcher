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

from neptune_fetcher.alpha.filters import (
    Attribute,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import context as _context
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.retrieval import search

__all__ = (
    "list_experiments",
    "list_runs",
)


def list_experiments(
    experiments: Optional[Union[str, Filter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
     Returns a list of experiment names in a project.

    `experiments` - a filter specifying which experiments to include
         - a regex that experiment name must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """
    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    return _list_containers(experiments, context, search.ContainerType.EXPERIMENT)


def list_runs(
    runs: Optional[Union[str, Filter]] = None,
    context: Optional[_context.Context] = None,
) -> list[str]:
    """
     Returns a list of run IDs in a project.

    `runs` - a filter specifying which runs to include
         - a regex that the run ID must match, or
         - a Filter object
    `context` - a Context object to be used; primarily useful for switching projects
    """
    if isinstance(runs, str):
        runs = Filter.matches_all(Attribute("sys/custom_run_id", type="string"), regex=runs)

    return _list_containers(runs, context, search.ContainerType.RUN)


def _list_containers(
    filter_: Optional[Filter],
    context: Optional[_context.Context],
    container_type: search.ContainerType,
) -> list[str]:
    validated_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(validated_context)
    project_identifier = identifiers.ProjectIdentifier(validated_context.project)  # type: ignore

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        if container_type == search.ContainerType.EXPERIMENT:
            exp_sys_pages = search.fetch_experiment_sys_attrs(client, project_identifier, filter_)
            return list(sorted(exp.sys_name for page in exp_sys_pages for exp in page.items))
        elif container_type == search.ContainerType.RUN:
            run_sys_pages = search.fetch_run_sys_attrs(client, project_identifier, filter_)
            return list(sorted(run.sys_custom_run_id for page in run_sys_pages for run in page.items))
        else:
            raise RuntimeError(f"Unexpected container type: {container_type}")
