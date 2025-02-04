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

import neptune_fetcher.alpha.filters as _filter
from neptune_fetcher.alpha.internal import attribute as _attribute
from neptune_fetcher.alpha.internal import identifiers as _identifiers
from neptune_fetcher.alpha.internal import util as _util


def infer_attribute_types_in_filter(
    client: AuthenticatedClient,
    project_identifier: _identifiers.ProjectIdentifier,
    experiment_filter: Optional[_filter.Filter],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
) -> None:
    if experiment_filter is None:
        return

    attributes = _filter_untyped(_walk_attributes(experiment_filter))
    if not attributes:
        return

    _infer_attribute_types_from_attribute(attributes)
    attributes = _filter_untyped(attributes)
    if not attributes:
        return

    _infer_attribute_types_from_api(
        client=client,
        project_identifier=project_identifier,
        experiment_filter=None,
        attributes=attributes,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
    )
    attributes = _filter_untyped(attributes)
    if attributes:
        raise ValueError(f"Failed to infer types for attributes: {', '.join(str(a) for a in attributes)}")


def infer_attribute_types_in_sort_by(
    client: AuthenticatedClient,
    project_identifier: _identifiers.ProjectIdentifier,
    experiment_filter: Optional[_filter.Filter],
    sort_by: _filter.Attribute,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
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
        experiment_filter=experiment_filter,
        attributes=attributes,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
    )
    attributes = _filter_untyped(attributes)
    if attributes:
        raise ValueError(f"Failed to infer types for attributes: {', '.join(str(a) for a in attributes)}")


def _infer_attribute_types_from_attribute(
    attributes: Iterable[_filter.Attribute],
) -> None:
    for attribute in attributes:
        if attribute.aggregation:
            attribute.type = "float_series"


def _infer_attribute_types_from_api(
    client: AuthenticatedClient,
    project_identifier: _identifiers.ProjectIdentifier,
    experiment_filter: Optional[_filter.Filter],
    attributes: Iterable[_filter.Attribute],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
) -> None:
    attribute_filter_by_name = _filter.AttributeFilter(name_eq=list({attr.name for attr in attributes}))

    output = _attribute.generate_attribute_definitions(
        client=client,
        project_identifier=project_identifier,
        experiment_filter=experiment_filter,
        attribute_filter=attribute_filter_by_name,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        downstream=_util.return_value,
    )

    attribute_definition_pages: Generator[
        _util.Page[_attribute.AttributeDefinition], None, None
    ] = _util.gather_results(output)

    attribute_name_to_definition: dict[str, set[str]] = defaultdict(set)
    for attribute_definition_page in attribute_definition_pages:
        for attr_def in attribute_definition_page.items:
            attribute_name_to_definition[attr_def.name].add(attr_def.type)

    for name, types in attribute_name_to_definition.items():
        if len(types) > 1:
            raise ValueError(f"Multiple type candidates found for attribute name '{name}': {', '.join(types)}")

    for attribute in attributes:
        if attribute.name in attribute_name_to_definition:
            attribute.type = next(iter(attribute_name_to_definition[attribute.name]))  # type: ignore


def _filter_untyped(
    attributes: Iterable[_filter.Attribute],
) -> list[_filter.Attribute]:
    return [attr for attr in attributes if attr.type is None]


def _walk_attributes(experiment_filter: _filter.Filter) -> Iterable[_filter.Attribute]:
    if isinstance(experiment_filter, _filter._AttributeValuePredicate):
        yield experiment_filter.attribute
    elif isinstance(experiment_filter, _filter._AttributePredicate):
        yield experiment_filter.attribute
    elif isinstance(experiment_filter, _filter._AssociativeOperator):
        for child in experiment_filter.filters:
            yield from _walk_attributes(child)
    elif isinstance(experiment_filter, _filter._PrefixOperator):
        yield from _walk_attributes(experiment_filter.filter_)
    else:
        raise ValueError(f"Unexpected filter type: {type(experiment_filter)}")
