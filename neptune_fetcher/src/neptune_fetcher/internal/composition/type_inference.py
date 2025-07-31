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
    Generic,
    Iterable,
    Optional,
    TypeVar,
)

from neptune_api.client import AuthenticatedClient

from ...exceptions import AttributeTypeInferenceError
from .. import (
    filters,
    identifiers,
)
from ..retrieval import search
from ..retrieval.attribute_types import (
    ATTRIBUTE_LITERAL,
    FILE_SERIES_AGGREGATIONS,
    FLOAT_SERIES_AGGREGATIONS,
    HISTOGRAM_SERIES_AGGREGATIONS,
    STRING_SERIES_AGGREGATIONS,
)
from ..retrieval.search import ContainerType
from .attributes import fetch_attribute_definitions

T = TypeVar("T", covariant=True)


KNOWN_ATTRIBUTE_TYPES: dict[str, ATTRIBUTE_LITERAL] = {
    "sys/archived": "bool",
    "sys/creation_time": "datetime",
    "sys/custom_run_id": "string",
    "sys/description": "string",
    "sys/diagnostics/attributes/bool_count": "int",
    "sys/diagnostics/attributes/file_ref_count": "int",
    "sys/diagnostics/attributes/file_ref_series_count": "int",
    "sys/diagnostics/attributes/float_count": "int",
    "sys/diagnostics/attributes/float_series_count": "int",
    "sys/diagnostics/attributes/histogram_count": "int",
    "sys/diagnostics/attributes/histogram_series_count": "int",
    "sys/diagnostics/attributes/int_count": "int",
    "sys/diagnostics/attributes/string_count": "int",
    "sys/diagnostics/attributes/string_series_count": "int",
    "sys/diagnostics/attributes/string_set_count": "int",
    "sys/diagnostics/attributes/total_count": "int",
    "sys/diagnostics/attributes/total_series_datapoints": "int",
    "sys/diagnostics/project_uuid": "string",
    "sys/diagnostics/run_uuid": "string",
    "sys/experiment/is_head": "bool",
    "sys/experiment/name": "string",
    "sys/experiment/running_time_seconds": "float",
    "sys/failed": "bool",
    "sys/family": "string",
    "sys/forking/depth": "int",
    "sys/group_tags": "string_set",
    "sys/id": "string",
    "sys/modification_time": "datetime",
    "sys/name": "string",
    "sys/owner": "string",
    "sys/ping_time": "datetime",
    "sys/relative_creation_time_ms": "int",
    "sys/running_time_seconds": "float",
    "sys/size": "int",
    "sys/tags": "string_set",
    "sys/trashed": "bool",
}


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
    result: T

    @staticmethod
    def empty() -> "InferenceState[None]":
        return InferenceState(attributes=[], result=None)

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

    def get_result_or_raise(self) -> T:
        self.raise_if_incomplete()
        return self.result


def infer_attribute_types_in_filter(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    executor: Executor,
    fetch_attribute_definitions_executor: Executor,
    container_type: search.ContainerType = search.ContainerType.EXPERIMENT,  # TODO: remove the default
) -> InferenceState[Optional[filters._Filter]]:
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
        filter_=None,  # filter_,
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
        matches: list[ATTRIBUTE_LITERAL] = []
        if attribute.name in KNOWN_ATTRIBUTE_TYPES:
            state.set_success(
                inferred_type=KNOWN_ATTRIBUTE_TYPES[attribute.name], success_details="Inferred from attribute name"
            )
            continue
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


# TODO: remove the unused parameters
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

    output = fetch_attribute_definitions(
        client=client,
        project_identifiers=[project_identifier],
        run_identifiers=None,
        attribute_filter=attribute_filter_by_name,
        executor=fetch_attribute_definitions_executor,
    )

    container_name = "runs" if container_type == ContainerType.RUN else "experiments"

    inferred_attributes = defaultdict(set)

    for page in output:
        for attr_def in page.items:
            for state in attribute_states:
                if state.attribute.name == attr_def.name:
                    inferred_attributes[state.attribute.name].add(attr_def.type)

    for state in attribute_states:
        types = inferred_attributes.get(state.attribute.name, set())
        if len(types) == 1:
            state.set_success(
                inferred_type=list(types)[0],  # type: ignore
                success_details="Inferred from neptune api",
            )

        if len(types) > 1:
            state.set_error(
                error=f"Neptune found the attribute name in multiple {container_name} "
                f"with conflicting types: {', '.join(types)}"
            )
