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

from abc import ABC
from dataclasses import (
    dataclass,
    field,
)
from datetime import datetime
from typing import (
    Iterable,
    Literal,
    Optional,
    Union,
)

__ALL__ = ("AttributeFilter",)


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return self.any(other)

    def any(*filters: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return _AttributeFilterAlternative(filters=filters)


@dataclass
class AttributeFilter(BaseAttributeFilter):
    name_eq: Union[str, list[str], None] = None
    type_in: Optional[
        list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]]
    ] = field(default_factory=lambda: ["float", "int", "string", "bool", "datetime", "float_series", "string_set"])
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: Optional[list[Literal["last", "min", "max", "average", "variance", "auto"]]] = None


@dataclass
class _AttributeFilterAlternative(BaseAttributeFilter):
    filters: tuple[BaseAttributeFilter]


@dataclass
class Attribute:
    name: str
    aggregation: Optional[Literal["last", "min", "max", "avg", "variance"]] = None
    type: Optional[Literal["bool", "int", "float", "string", "datetime", "float_series", "string_set"]] = None

    def to_query(self) -> str:
        if self.aggregation is not None:
            return f"{self.aggregation}(`{self.name}`:{self.type})"
        else:
            return f"`{self.name}`:{self.type}"


class ExperimentFilter(ABC):
    @staticmethod
    def eq(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator="==", attribute=attribute, value=value)

    @staticmethod
    def ne(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator="!=", attribute=attribute, value=value)

    @staticmethod
    def gt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator=">", attribute=attribute, value=value)

    @staticmethod
    def ge(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator=">=", attribute=attribute, value=value)

    @staticmethod
    def lt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator="<", attribute=attribute, value=value)

    @staticmethod
    def le(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "ExperimentFilter":
        return _AttributeValuePredicate(operator="<=", attribute=attribute, value=value)

    @staticmethod
    def matches_all(attribute: Union[str, Attribute], regex: Union[str, list[str]]) -> "ExperimentFilter":
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="MATCHES", attribute=attribute, value=regex)
        else:
            filters = [ExperimentFilter.matches_all(attribute, r) for r in regex]
            return ExperimentFilter.all(*filters)

    @staticmethod
    def matches_none(attribute: Union[str, Attribute], regex: Union[str, list[str]]) -> "ExperimentFilter":
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="NOT MATCHES", attribute=attribute, value=regex)
        else:
            filters = [ExperimentFilter.matches_none(attribute, r) for r in regex]
            return ExperimentFilter.any(*filters)

    @staticmethod
    def contains_all(attribute: Union[str, Attribute], value: Union[str, list[str]]) -> "ExperimentFilter":
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="CONTAINS", attribute=attribute, value=value)
        else:
            filters = [ExperimentFilter.contains_all(attribute, v) for v in value]
            return ExperimentFilter.all(*filters)

    @staticmethod
    def contains_none(attribute: Union[str, Attribute], value: Union[str, list[str]]) -> "ExperimentFilter":
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="NOT CONTAINS", attribute=attribute, value=value)
        else:
            filters = [ExperimentFilter.contains_none(attribute, v) for v in value]
            return ExperimentFilter.any(*filters)

    @staticmethod
    def exists(attribute: Union[str, Attribute]) -> "ExperimentFilter":
        return _AttributePredicate(postfix_operator="EXISTS", attribute=attribute)

    @staticmethod
    def all(*filters: "ExperimentFilter") -> "ExperimentFilter":
        return _AssociativeOperator(operator="AND", filters=filters)

    @staticmethod
    def any(*filters: "ExperimentFilter") -> "ExperimentFilter":
        return _AssociativeOperator(operator="OR", filters=filters)

    @staticmethod
    def negate(filter_: "ExperimentFilter") -> "ExperimentFilter":
        return _PrefixOperator(operator="NOT", filter_=filter_)

    def __and__(self, other: "ExperimentFilter") -> "ExperimentFilter":
        return self.all(self, other)

    def __or__(self, other: "ExperimentFilter") -> "ExperimentFilter":
        return self.any(self, other)

    def __invert__(self) -> "ExperimentFilter":
        return self.negate(self)

    @staticmethod
    def name_eq(name: str) -> "ExperimentFilter":
        name_attribute = Attribute(name="sys/name", type="string")
        return ExperimentFilter.eq(name_attribute, name)

    @staticmethod
    def name_in(*names: str) -> "ExperimentFilter":
        if len(names) == 1:
            return ExperimentFilter.name_eq(names[0])
        else:
            filters = [ExperimentFilter.name_eq(name) for name in names]
            return ExperimentFilter.any(*filters)

    def to_query(self) -> str:
        ...


@dataclass
class _AttributeValuePredicate(ExperimentFilter):
    operator: str
    attribute: Union[str, Attribute]
    value: Union[int, float, str, datetime]

    def to_query(self) -> str:
        return f"{self._left_query()} {self.operator} {self._right_query()}"

    def _left_query(self):
        if isinstance(self.attribute, Attribute):
            return self.attribute.to_query()
        return self.attribute

    def _right_query(self):
        value = self.value.replace("\\", r"\\").replace('"', r"\"")
        return f'"{value}"'


@dataclass
class _AttributePredicate(ExperimentFilter):
    postfix_operator: str
    attribute: Union[str, Attribute]

    def to_query(self) -> str:
        return f"{self.attribute.to_query()} {self.postfix_operator}"


@dataclass
class _AssociativeOperator(ExperimentFilter):
    operator: str
    filters: Iterable[ExperimentFilter]

    def to_query(self) -> str:
        filter_queries = [f"({child.to_query()})" for child in self.filters]
        return f" {self.operator} ".join(filter_queries)


@dataclass
class _PrefixOperator(ExperimentFilter):
    operator: str
    filter_: ExperimentFilter

    def to_query(self) -> str:
        return f"{self.operator} ({self.filter_.to_query()})"
