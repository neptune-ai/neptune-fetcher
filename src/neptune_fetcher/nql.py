#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from __future__ import annotations

__all__ = [
    "NQLQuery",
    "NQLEmptyQuery",
    "NQLAggregator",
    "NQLQueryAggregate",
    "NQLAttributeOperator",
    "NQLAttributeType",
    "NQLQueryAttribute",
    "RawNQLQuery",
    "prepare_nql_query",
]

import typing
from dataclasses import dataclass
from enum import Enum
from typing import (
    Iterable,
    List,
    Optional,
    Union,
)

from neptune_fetcher.util import escape_nql_criterion


@dataclass
class NQLQuery:
    def eval(self) -> NQLQuery:
        return self


@dataclass
class NQLEmptyQuery(NQLQuery):
    def __str__(self) -> str:
        return ""


class NQLAggregator(str, Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class NQLQueryAggregate(NQLQuery):
    items: Iterable[NQLQuery]
    aggregator: NQLAggregator

    def eval(self) -> NQLQuery:
        self.items = list(filter(lambda nql: not isinstance(nql, NQLEmptyQuery), (item.eval() for item in self.items)))

        if len(self.items) == 0:
            return NQLEmptyQuery()
        elif len(self.items) == 1:
            return self.items[0]
        return self

    def __str__(self) -> str:
        evaluated = self.eval()
        if isinstance(evaluated, NQLQueryAggregate):
            return "(" + f" {self.aggregator.value} ".join(map(str, self.items)) + ")"
        return str(evaluated)


class NQLAttributeOperator(str, Enum):
    EQUALS = "="
    CONTAINS = "CONTAINS"
    GREATER_THAN = ">"
    LESS_THAN = "<"
    MATCHES = "MATCHES"
    NOT_MATCHES = "NOT MATCHES"


class NQLAttributeType(str, Enum):
    STRING = "string"
    STRING_SET = "stringSet"
    EXPERIMENT_STATE = "experimentState"
    BOOLEAN = "bool"
    DATETIME = "datetime"
    INTEGER = "integer"
    FLOAT = "float"


@dataclass
class NQLQueryAttribute(NQLQuery):
    name: str
    type: NQLAttributeType
    operator: NQLAttributeOperator
    value: typing.Union[str, bool]

    def __str__(self) -> str:
        if isinstance(self.value, bool):
            value = str(self.value).lower()
        else:
            value = f'"{self.value}"'

        return f"(`{self.name}`:{self.type.value} {self.operator.value} {value})"


@dataclass
class RawNQLQuery(NQLQuery):
    query: str

    def eval(self) -> NQLQuery:
        if self.query == "":
            return NQLEmptyQuery()
        return self

    def __str__(self) -> str:
        evaluated = self.eval()
        if isinstance(evaluated, RawNQLQuery):
            return self.query
        return str(evaluated)


class RunState(Enum):
    active = "Active"
    inactive = "Inactive"

    _api_active = "running"
    _api_inactive = "idle"

    @classmethod
    def from_string(cls, value: str) -> "RunState":
        try:
            return cls(value.capitalize())
        except ValueError as e:
            raise Exception(f"Can't map RunState to API: {value}") from e

    @staticmethod
    def from_api(value: str) -> "RunState":
        if value == RunState._api_active.value:
            return RunState.active
        elif value == RunState._api_inactive.value:
            return RunState.inactive
        else:
            raise Exception(f"Unknown RunState: {value}")

    def to_api(self) -> str:
        if self is RunState.active:
            return self._api_active.value
        if self is RunState.inactive:
            return self._api_inactive.value


def prepare_nql_query(
    ids: Optional[Iterable[str]],
    states: Optional[Iterable[str]],
    owners: Optional[Iterable[str]],
    tags: Optional[Iterable[str]],
    trashed: Optional[bool],
) -> NQLQueryAggregate:
    query_items: List[Union[NQLQueryAttribute, NQLQueryAggregate]] = []

    if trashed is not None:
        query_items.append(
            NQLQueryAttribute(
                name="sys/trashed",
                type=NQLAttributeType.BOOLEAN,
                operator=NQLAttributeOperator.EQUALS,
                value=trashed,
            )
        )

    if ids:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/id",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.EQUALS,
                        value=escape_nql_criterion(api_id),
                    )
                    for api_id in ids
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if states:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/state",
                        type=NQLAttributeType.EXPERIMENT_STATE,
                        operator=NQLAttributeOperator.EQUALS,
                        value=RunState.from_string(state).to_api(),
                    )
                    for state in states
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if owners:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/owner",
                        type=NQLAttributeType.STRING,
                        operator=NQLAttributeOperator.EQUALS,
                        value=owner,
                    )
                    for owner in owners
                ],
                aggregator=NQLAggregator.OR,
            )
        )

    if tags:
        query_items.append(
            NQLQueryAggregate(
                items=[
                    NQLQueryAttribute(
                        name="sys/tags",
                        type=NQLAttributeType.STRING_SET,
                        operator=NQLAttributeOperator.CONTAINS,
                        value=tag,
                    )
                    for tag in tags
                ],
                aggregator=NQLAggregator.AND,
            )
        )

    query = NQLQueryAggregate(items=query_items, aggregator=NQLAggregator.AND)
    return query


def build_raw_query(query: str, trashed: typing.Optional[bool]) -> NQLQuery:
    raw_nql = RawNQLQuery(query)

    if trashed is None:
        return raw_nql

    nql = NQLQueryAggregate(
        items=[
            raw_nql,
            NQLQueryAttribute(
                name="sys/trashed", type=NQLAttributeType.BOOLEAN, operator=NQLAttributeOperator.EQUALS, value=trashed
            ),
        ],
        aggregator=NQLAggregator.AND,
    )
    return nql
