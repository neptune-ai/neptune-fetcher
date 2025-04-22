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

from collections import defaultdict
from concurrent.futures import Executor
from typing import (
    Generator,
    Iterable,
    Optional,
)

from neptune_api.client import AuthenticatedClient

from neptune_fetcher.alpha import filters
from neptune_fetcher.alpha.exceptions import AttributeTypeInferenceError
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.alpha.internal.retrieval import (
    search,
    util,
)


def infer_attribute_types_in_filter(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters.Filter],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType = search.ContainerType.EXPERIMENT,  # TODO: remove the default
) -> None:
    if filter_ is None:
        return

    attributes = _filter_untyped(_walk_attributes(filter_))
    if not attributes:
        return

    _infer_attribute_types_from_attribute(attributes)
    attributes = _filter_untyped(attributes)
    if not attributes:
        return

    _infer_attribute_types_from_api(
        client=client,
        project_identifier=project_identifier,
        filter_=None,
        attributes=attributes,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        container_type=container_type,
    )
    attributes = _filter_untyped(attributes)
    if attributes:
        raise AttributeTypeInferenceError(attribute_names=[a.name for a in attributes])


def infer_attribute_types_in_sort_by(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters.Filter],
    sort_by: filters.Attribute,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType = search.ContainerType.EXPERIMENT,  # TODO: remove the default
) -> None:
    attributes = _filter_untyped([sort_by])
    if not attributes:
        return

    _infer_attribute_types_from_attribute(attributes)
    attributes = _filter_untyped(attributes)
    if not attributes:
        return

    _infer_attribute_types_from_api(
        client=client,
        project_identifier=project_identifier,
        filter_=filter_,
        attributes=attributes,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        container_type=container_type,
    )
    attributes = _filter_untyped(attributes)
    if attributes:
        raise AttributeTypeInferenceError(attribute_names=[a.name for a in attributes])


def _infer_attribute_types_from_attribute(
    attributes: Iterable[filters.Attribute],
) -> None:
    for attribute in attributes:
        if attribute.aggregation:
            attribute.type = "float_series"


def _infer_attribute_types_from_api(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters.Filter],
    attributes: Iterable[filters.Attribute],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType,
) -> None:
    attribute_filter_by_name = filters.AttributeFilter(name_eq=list({attr.name for attr in attributes}))

    output = _components.fetch_attribute_definitions_complete(
        client=client,
        project_identifier=project_identifier,
        filter_=filter_,
        attribute_filter=attribute_filter_by_name,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        container_type=container_type,
        downstream=concurrency.return_value,
    )

    attribute_definition_pages: Generator[
        util.Page[att_defs.AttributeDefinition], None, None
    ] = concurrency.gather_results(output)

    attribute_name_to_definition: dict[str, set[str]] = defaultdict(set)
    for attribute_definition_page in attribute_definition_pages:
        for attr_def in attribute_definition_page.items:
            attribute_name_to_definition[attr_def.name].add(attr_def.type)

    for name, types in attribute_name_to_definition.items():
        if len(types) > 1:
            raise AttributeTypeInferenceError(attribute_names=[name])

    for attribute in attributes:
        if attribute.name in attribute_name_to_definition:
            attribute.type = next(iter(attribute_name_to_definition[attribute.name]))  # type: ignore


def _filter_untyped(
    attributes: Iterable[filters.Attribute],
) -> list[filters.Attribute]:
    return [attr for attr in attributes if attr.type is None]


def _walk_attributes(experiment_filter: filters.Filter) -> Iterable[filters.Attribute]:
    if isinstance(experiment_filter, filters._AttributeValuePredicate):
        yield experiment_filter.attribute
    elif isinstance(experiment_filter, filters._AttributePredicate):
        yield experiment_filter.attribute
    elif isinstance(experiment_filter, filters._AssociativeOperator):
        for child in experiment_filter.filters:
            yield from _walk_attributes(child)
    elif isinstance(experiment_filter, filters._PrefixOperator):
        yield from _walk_attributes(experiment_filter.filter_)
    else:
        raise RuntimeError(f"Unexpected filter type: {type(experiment_filter)}")
