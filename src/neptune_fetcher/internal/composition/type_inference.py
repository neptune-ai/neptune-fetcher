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
import copy
from collections import defaultdict
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import (
    Generator,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

from neptune_api.client import AuthenticatedClient

from neptune_fetcher.exceptions import AttributeTypeInferenceError
from neptune_fetcher.internal import (
    filters,
    identifiers,
)
from neptune_fetcher.internal.composition import attribute_components as _components
from neptune_fetcher.internal.composition import concurrency
from neptune_fetcher.internal.filters import ATTRIBUTE_LITERAL
from neptune_fetcher.internal.retrieval import (
    search,
    util,
)
from neptune_fetcher.internal.retrieval.attribute_types import (
    FILE_SERIES_AGGREGATIONS,
    FLOAT_SERIES_AGGREGATIONS,
    HISTOGRAM_SERIES_AGGREGATIONS,
    STRING_SERIES_AGGREGATIONS,
)
from neptune_fetcher.internal.retrieval.search import ContainerType

T = TypeVar("T")


@dataclass
class AttributeInferenceState:
    original_attribute: filters._Attribute
    attribute: filters._Attribute
    inferred_type: Optional[str] = None
    success_details: Optional[str] = None
    error: Optional[str] = None

    def is_finalized(self) -> bool:
        return self.inferred_type is not None or self.error is not None

    def is_inferred(self) -> bool:
        return self.inferred_type is not None

    def set_success(self, inferred_type: ATTRIBUTE_LITERAL, success_details: Optional[str] = None) -> None:
        self.inferred_type = inferred_type
        self.success_details = success_details
        self.attribute.type = inferred_type

    def set_error(self, error: str) -> None:
        self.error = error


@dataclass
class InferenceState(Generic[T]):
    attributes: list[AttributeInferenceState]
    run_domain_empty: Optional[bool] = None
    result: Optional[T] = None

    @staticmethod
    def empty() -> "InferenceState":
        return InferenceState(attributes=[])

    @staticmethod
    def from_attribute(attribute: filters._Attribute) -> "InferenceState[filters._Attribute]":
        attribute_copy = copy.deepcopy(attribute)

        attribute_states = [
            AttributeInferenceState(
                original_attribute=copy.copy(attribute_copy),
                attribute=attribute_copy,
                inferred_type=attribute_copy.type,
                success_details="Type provided" if attribute_copy.type is not None else None,
            )
        ]
        return InferenceState(attributes=attribute_states, result=attribute_copy)

    @staticmethod
    def from_filter(filter_: filters._Filter) -> "InferenceState[filters._Filter]":
        def _walk_attributes(experiment_filter: filters._Filter) -> Iterable[filters._Attribute]:
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

        filter_copy = copy.deepcopy(filter_)

        attributes = _walk_attributes(filter_copy)
        attribute_states = [
            AttributeInferenceState(
                original_attribute=copy.copy(attr),
                attribute=attr,
                inferred_type=attr.type,
                success_details="Type provided" if attr.type is not None else None,
            )
            for attr in attributes
        ]
        return InferenceState(attributes=attribute_states, result=filter_copy)

    def incomplete_attributes(self) -> list[AttributeInferenceState]:
        return [attr_state for attr_state in self.attributes if not attr_state.is_finalized()]

    def is_complete(self) -> bool:
        return all(attr_state.is_finalized() for attr_state in self.attributes)

    def is_run_domain_empty(self) -> bool:
        return self.run_domain_empty is not None and self.run_domain_empty

    def raise_if_incomplete(self) -> None:
        uninferred_attributes = [attr_state for attr_state in self.attributes if not attr_state.is_inferred()]
        if uninferred_attributes:
            attribute_names = [attr_state.original_attribute.name for attr_state in uninferred_attributes]

            details = []
            for attr_state in uninferred_attributes:
                if attr_state.error:
                    details.append(f"{attr_state.original_attribute.name}: {attr_state.error}")
                else:
                    details.append(f"{attr_state.original_attribute.name}: could not find the attribute")

            raise AttributeTypeInferenceError(attribute_names=attribute_names, details=details)

    def get_result_or_raise(self) -> Optional[T]:
        self.raise_if_incomplete()
        return self.result


def infer_attribute_types_in_filter(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType = search.ContainerType.EXPERIMENT,  # TODO: remove the default
) -> InferenceState:
    if filter_ is None:
        return InferenceState.empty()

    state = InferenceState.from_filter(filter_)
    if state.is_complete():
        return state

    _infer_attribute_types_locally(inference_state=state)
    if state.is_complete():
        return state

    _infer_attribute_types_from_api(
        client=client,
        project_identifier=project_identifier,
        filter_=None,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        container_type=container_type,
        inference_state=state,
    )
    return state


def infer_attribute_types_in_sort_by(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    sort_by: filters._Attribute,
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType = search.ContainerType.EXPERIMENT,  # TODO: remove the default
) -> InferenceState[filters._Attribute]:
    state = InferenceState.from_attribute(sort_by)
    if state.is_complete():
        return state

    _infer_attribute_types_locally(inference_state=state)
    if state.is_complete():
        return state

    _infer_attribute_types_from_api(
        client=client,
        project_identifier=project_identifier,
        filter_=filter_,
        executor=executor,
        fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        container_type=container_type,
        inference_state=state,
    )
    return state


def _infer_attribute_types_locally(
    inference_state: InferenceState,
) -> None:
    for state in inference_state.incomplete_attributes():
        attribute = state.attribute
        matches = []
        if all(agg in FLOAT_SERIES_AGGREGATIONS for agg in attribute.aggregation or []):
            matches.append("float_series")
        if all(agg in STRING_SERIES_AGGREGATIONS for agg in attribute.aggregation or []):
            matches.append("string_series")
        if all(agg in FILE_SERIES_AGGREGATIONS for agg in attribute.aggregation or []):
            matches.append("file_series")
        if all(agg in HISTOGRAM_SERIES_AGGREGATIONS for agg in attribute.aggregation or []):
            matches.append("histogram_series")
        if len(matches) == 1:
            state.set_success(inferred_type=matches[0], success_details="Inferred from aggregation")


def _infer_attribute_types_from_api(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType,
    inference_state: InferenceState,
) -> None:
    attribute_states = inference_state.incomplete_attributes()
    attributes = [state.attribute for state in attribute_states]
    attribute_filter_by_name = filters._AttributeFilter(name_eq=list({attr.name for attr in attributes}))

    output = concurrency.generate_concurrently(
        items=search.fetch_sys_ids(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            container_type=container_type,
        ),
        executor=executor,
        downstream=lambda sys_ids_page: concurrency.fork_concurrently(
            executor=executor,
            downstreams=[
                lambda: _components.fetch_attribute_definitions_split(
                    client=client,
                    project_identifier=project_identifier,
                    attribute_filter=attribute_filter_by_name,
                    executor=executor,
                    fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                    sys_ids=sys_ids_page.items,
                    downstream=lambda _, definitions: concurrency.return_value(definitions),
                ),
                lambda: concurrency.return_value(sys_ids_page.items),
            ],
        ),
    )

    results: Generator[
        Union[list[identifiers.SysId], util.Page[identifiers.AttributeDefinition]], None, None
    ] = concurrency.gather_results(output)

    sys_ids: list[identifiers.SysId] = []
    attribute_name_to_definition: dict[str, set[str]] = defaultdict(set)
    for result in results:
        if isinstance(result, util.Page):
            for attr_def in result.items:
                attribute_name_to_definition[attr_def.name].add(attr_def.type)
        elif isinstance(result, list):
            sys_ids.extend(result)

    for state in attribute_states:
        attribute = state.attribute
        if attribute.name in attribute_name_to_definition:
            types = attribute_name_to_definition[attribute.name]
            if len(types) == 1:
                state.set_success(
                    inferred_type=next(iter(types)),  # type: ignore
                    success_details="Inferred from neptune api",
                )
            elif len(types) > 1:
                container_name = "runs" if container_type == ContainerType.RUN else "experiments"
                state.set_error(
                    error=f"Neptune found the attribute name in multiple {container_name} "
                    f"with conflicting types: {', '.join(types)}"
                )

    inference_state.run_domain_empty = len(sys_ids) == 0
