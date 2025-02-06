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

__all__ = ("list_attributes",)

from concurrent.futures import Executor
from typing import (
    Generator,
    Optional,
    Union,
)

from neptune_fetcher.alpha.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    BaseAttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import identifiers as _identifiers
from neptune_fetcher.alpha.internal.api_client import attribute_definitions as _adef
from neptune_fetcher.alpha.internal.api_client import client as _client
from neptune_fetcher.alpha.internal.api_client import search as _search
from neptune_fetcher.alpha.internal.api_client import split as _split
from neptune_fetcher.alpha.internal.api_client import util as _util
from neptune_fetcher.alpha.internal.composition import concurrency as _concurrency
from neptune_fetcher.alpha.internal.composition import type_inference as _infer


def list_attributes(
    experiments: Optional[Union[str, Filter]] = None,
    attributes: Optional[Union[str, AttributeFilter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
    List attributes' names in project.
    Optionally filter by experiments and attributes.
    `experiments` - a filter specifying experiments to which the attributes belong
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
            If `AttributeFilter.aggregations` is set, an exception will be raised as they're
            not supported in this function.
    `context` - a Context object to be used; primarily useful for switching projects

    Returns a list of unique attribute names in experiments matching the filter.
    """

    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    assert valid_context.project is not None  # mypy TODO: remove at some point
    project_identifier = _identifiers.ProjectIdentifier(valid_context.project)

    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    if attributes is None:
        attributes = AttributeFilter()
    elif isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=[attributes])

    with (
        _concurrency.create_thread_pool_executor() as executor,
        _concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        _infer.infer_attribute_types_in_filter(
            client,
            project_identifier,
            experiments,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        result = _list_attributes(
            client,
            project_identifier,
            experiment_filter=experiments,
            attribute_filter=attributes,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        return sorted(set(result))


def _list_attributes(
    client: _client.AuthenticatedClient,
    project_id: _identifiers.ProjectIdentifier,
    experiment_filter: Optional[Filter],
    attribute_filter: BaseAttributeFilter,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
) -> Generator[str, None, None]:
    if experiment_filter is not None:
        output = _concurrency.generate_concurrently(
            items=_search.fetch_experiment_sys_attrs(
                client,
                project_identifier=project_id,
                experiment_filter=experiment_filter,
            ),
            executor=executor,
            downstream=lambda experiments_page: _concurrency.generate_concurrently(
                items=_split.split_experiments(
                    [_identifiers.ExperimentIdentifier(project_id, e.sys_id) for e in experiments_page.items]
                ),
                executor=executor,
                downstream=lambda experiment_identifier_split: _concurrency.generate_concurrently(
                    items=_adef.fetch_attribute_definitions(
                        client,
                        [project_id],
                        experiment_identifier_split,
                        attribute_filter,
                        fetch_attribute_definitions_executor,
                    ),
                    executor=executor,
                    downstream=_concurrency.return_value,
                ),
            ),
        )
    else:
        output = _concurrency.generate_concurrently(
            items=_adef.fetch_attribute_definitions(
                client,
                [project_id],
                None,
                attribute_filter,
                fetch_attribute_definitions_executor,
            ),
            executor=executor,
            downstream=_concurrency.return_value,
        )

    results: Generator[_util.Page[_adef.AttributeDefinition], None, None] = _concurrency.gather_results(output)
    for page in results:
        for item in page.items:
            yield item.name
