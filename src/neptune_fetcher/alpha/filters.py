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
import abc
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

from neptune_fetcher.internal import filters as _filters
from neptune_fetcher.internal.filters import _AttributeNameFilter
from neptune_fetcher.internal.retrieval import attribute_types as types
from neptune_fetcher.internal.util import (
    _validate_allowed_value,
    _validate_list_of_allowed_values,
    _validate_string_or_string_list,
)

__all__ = ["Filter", "AttributeFilter", "Attribute"]


ATTRIBUTE_LITERAL = Literal[
    "float", "int", "string", "bool", "datetime", "float_series", "string_set", "string_series", "file"
]
AGGREGATION_LITERAL = Literal["last", "min", "max", "average", "variance"]


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return BaseAttributeFilter.any(self, other)

    @staticmethod
    def any(*filters: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return _AttributeFilterAlternative(filters=filters)

    @abc.abstractmethod
    def _to_internal(self) -> _filters._BaseAttributeFilter:
        ...


@dataclass
class AttributeFilter(BaseAttributeFilter):
    """Filter to apply to attributes when fetching runs or experiments.

    Use to select specific metrics or other metadata based on various criteria.

    Args:
        name_eq (Union[str, list[str], None]): An attribute name or list of names to match exactly.
            If `None`, this filter is not applied.
        type_in (list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set",
        "string_series", "file"]]):
            A list of allowed attribute types. Defaults to all available types.
            For a reference, see: https://docs.neptune.ai/attribute_types
        name_matches_all (Union[str, list[str], None]): A regular expression or list of expressions that the attribute
            name must match. If `None`, this filter is not applied.
        name_matches_none (Union[str, list[str], None]): A regular expression or list of expressions that the attribute
            names mustn't match. Attributes matching any of the regexes are excluded.
            If `None`, this filter is not applied.
        aggregations (list[Literal["last", "min", "max", "average", "variance"]]): List of
            aggregation functions to apply when fetching metrics of type FloatSeries or StringSeries.
            Defaults to ["last"].

    Example:

    ```
    import neptune_fetcher.alpha as npt
    from neptune_fetcher.alpha.filters import AttributeFilter


    loss_avg_and_var = AttributeFilter(
        type_in=["float_series"],
        name_matches_all=[r"loss$"],
        aggregations=["average", "variance"],
    )

    npt.fetch_experiments_table(attributes=loss_avg_and_var)
    ```
    """

    name_eq: Union[str, list[str], None] = None
    type_in: list[ATTRIBUTE_LITERAL] = field(default_factory=lambda: list(types.ALL_TYPES))  # type: ignore
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: list[AGGREGATION_LITERAL] = field(default_factory=lambda: ["last"])

    def __post_init__(self) -> None:
        _validate_string_or_string_list(self.name_eq, "name_eq")
        _validate_string_or_string_list(self.name_matches_all, "name_matches_all")
        _validate_string_or_string_list(self.name_matches_none, "name_matches_none")

        _validate_list_of_allowed_values(self.type_in, types.ALL_TYPES, "type_in")
        _validate_list_of_allowed_values(self.aggregations, types.ALL_AGGREGATIONS, "aggregations")

    def _to_internal(self) -> _filters._AttributeFilter:
        matches_all = [self.name_matches_all] if isinstance(self.name_matches_all, str) else self.name_matches_all
        matches_none = [self.name_matches_none] if isinstance(self.name_matches_none, str) else self.name_matches_none

        if matches_all is not None or matches_none is not None:
            must_match_any = [
                _AttributeNameFilter(
                    must_match_regexes=matches_all,
                    must_not_match_regexes=matches_none,
                )
            ]
        else:
            must_match_any = None

        return _filters._AttributeFilter(
            name_eq=self.name_eq,
            type_in=self.type_in,
            must_match_any=must_match_any,
            aggregations=self.aggregations,
        )


@dataclass
class _AttributeFilterAlternative(BaseAttributeFilter):
    filters: Iterable[BaseAttributeFilter]

    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        filters = tuple(self.filters) + (other,)
        return _AttributeFilterAlternative(filters)

    def _to_internal(self) -> _filters._AttributeFilterAlternative:
        return _filters._AttributeFilterAlternative(
            filters=[f._to_internal() for f in self.filters],
        )


@dataclass
class Attribute:
    """Helper for specifying an attribute and picking a metric aggregation function.

    When fetching experiments or runs, use this class to filter and sort the returned entries.

    Args:
        name (str): An attribute name to match exactly.
        aggregation (Literal["last", "min", "max", "average", "variance"], optional):
            Aggregation function to apply when specifying a metric of type FloatSeries.
            Defaults to `"last"`, i.e. the last logged value.
        type (Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"], optional):
            Attribute type. Specify it to resolve ambiguity, in case some of the project's runs contain attributes
            that have the same name but are of a different type.
            For a reference, see: https://docs.neptune.ai/attribute_types

    Example:

    Select a metric and pick variance as the aggregation:

    ```
    import neptune_fetcher.alpha as npt
    from neptune_fetcher.alpha.filters import Attribute, Filter


    val_loss_variance = Attribute(
        name="val/loss",
        aggregation="variance",
    )
    # Construct a filter and pass it to a fetching or listing method
    tiny_val_loss_variance = Filter.lt(val_loss_variance, 0.01)
    npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
    ```
    """

    name: str
    aggregation: Optional[AGGREGATION_LITERAL] = None
    type: Optional[ATTRIBUTE_LITERAL] = None

    def __post_init__(self) -> None:
        _validate_allowed_value(self.aggregation, types.ALL_AGGREGATIONS, "aggregation")
        _validate_allowed_value(self.type, types.ALL_TYPES, "type")

    def to_query(self) -> str:
        query = f"`{self.name}`"

        if self.type is not None:
            _type = types.map_attribute_type_python_to_backend(self.type)
            query += f":{_type}"

        if self.aggregation is not None:
            query = f"{self.aggregation}({query})"

        return query

    def _to_internal(self) -> _filters._Attribute:
        return _filters._Attribute(
            name=self.name,
            aggregation=self.aggregation,
            type=self.type,
        )

    def __str__(self) -> str:
        return self.to_query()


class Filter(ABC):
    """Filter used to specify criteria when fetching experiments or runs.

    Examples of filters:
        - Name or attribute must match regular expression.
        - Attribute value must pass a condition, like "greater than 0.9".

    You can negate a filter or join multiple filters with logical operators.

    Methods available for attribute values:
    - `name_eq()`: Run or experiment name equals
    - `name_in()`: Run or experiment name equals any of the provided names
    - `eq()`: Value equals
    - `ne()`: Value doesn't equal
    - `gt()`: Value is greater than
    - `ge()`: Value is greater than or equal to
    - `lt()`: Value is less than
    - `le()`: Value is less than or equal to
    - `matches_all()`: Value matches regex or all in list of regexes
    - `matches_none()`: Value doesn't match regex or any of list of regexes
    - `contains_all()`: Tagset contains all tags, or string contains substrings
    - `contains_none()`: Tagset doesn't contain any of the tags, or string doesn't contain the substrings
    - `exists()`: Attribute exists

    Examples:

    ```
    import neptune_fetcher.alpha as npt
    from neptune_fetcher.alpha.filters import Filter

    # Fetch metadata from specific experiments
    specific_experiments = Filter.name_in("flying-123", "swimming-77")
    npt.fetch_experiments_table(experiments=specific_experiments)

    # Define various criteria
    owned_by_me = Filter.eq("sys/owner", "vidar")
    loss_filter = Filter.lt("validation/loss", 0.1)
    tag_filter = Filter.contains_none("sys/tags", ["test", "buggy"])
    dataset_check = Filter.exists("dataset_version")

    my_interesting_experiments = owned_by_me & loss_filter & tag_filter & dataset_check
    npt.fetch_experiments_table(experiments=my_interesting_experiments)
    ```
    """

    @staticmethod
    def eq(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator="==", attribute=attribute, value=value)

    @staticmethod
    def ne(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator="!=", attribute=attribute, value=value)

    @staticmethod
    def gt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator=">", attribute=attribute, value=value)

    @staticmethod
    def ge(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator=">=", attribute=attribute, value=value)

    @staticmethod
    def lt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator="<", attribute=attribute, value=value)

    @staticmethod
    def le(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributeValuePredicate(operator="<=", attribute=attribute, value=value)

    @staticmethod
    def matches_all(attribute: Union[str, Attribute], regex: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="MATCHES", attribute=attribute, value=regex)
        else:
            filters = [Filter.matches_all(attribute, r) for r in regex]
            return Filter.all(*filters)

    @staticmethod
    def matches_none(attribute: Union[str, Attribute], regex: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="NOT MATCHES", attribute=attribute, value=regex)
        else:
            filters = [Filter.matches_none(attribute, r) for r in regex]
            return Filter.all(*filters)

    @staticmethod
    def contains_all(attribute: Union[str, Attribute], value: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="CONTAINS", attribute=attribute, value=value)
        else:
            filters = [Filter.contains_all(attribute, v) for v in value]
            return Filter.all(*filters)

    @staticmethod
    def contains_none(attribute: Union[str, Attribute], value: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="NOT CONTAINS", attribute=attribute, value=value)
        else:
            filters = [Filter.contains_none(attribute, v) for v in value]
            return Filter.all(*filters)

    @staticmethod
    def exists(attribute: Union[str, Attribute]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return _AttributePredicate(postfix_operator="EXISTS", attribute=attribute)

    @staticmethod
    def all(*filters: "Filter") -> "Filter":
        return _AssociativeOperator(operator="AND", filters=filters)

    @staticmethod
    def any(*filters: "Filter") -> "Filter":
        return _AssociativeOperator(operator="OR", filters=filters)

    @staticmethod
    def negate(filter_: "Filter") -> "Filter":
        return _PrefixOperator(operator="NOT", filter_=filter_)

    def __and__(self, other: "Filter") -> "Filter":
        return self.all(self, other)

    def __or__(self, other: "Filter") -> "Filter":
        return self.any(self, other)

    def __invert__(self) -> "Filter":
        return self.negate(self)

    @staticmethod
    def name_eq(name: str) -> "Filter":
        name_attribute = Attribute(name="sys/name", type="string")
        return Filter.eq(name_attribute, name)

    @staticmethod
    def name_in(*names: str) -> "Filter":
        if len(names) == 1:
            return Filter.name_eq(names[0])
        else:
            filters = [Filter.name_eq(name) for name in names]
            return Filter.any(*filters)

    def to_query(self) -> str:
        return self._to_internal().to_query()

    @abc.abstractmethod
    def _to_internal(self) -> _filters._Filter:
        ...

    def __str__(self) -> str:
        return self.to_query()


@dataclass
class _AttributeValuePredicate(Filter):
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "MATCHES", "NOT MATCHES", "CONTAINS", "NOT CONTAINS"]
    attribute: Attribute
    value: Union[bool, int, float, str, datetime]

    def __post_init__(self) -> None:
        allowed_operators = {"==", "!=", ">", ">=", "<", "<=", "MATCHES", "NOT MATCHES", "CONTAINS", "NOT CONTAINS"}
        _validate_allowed_value(self.operator, allowed_operators, "operator")

        if not isinstance(self.value, (bool, int, float, str, datetime)):
            raise TypeError(f"Invalid value type: {type(self.value).__name__}. Expected int, float, str, or datetime.")

    def _right_query(self) -> str:
        if isinstance(self.value, datetime):
            return f'"{self.value.astimezone().isoformat()}"'
        value = str(self.value)
        value = value.replace("\\", r"\\").replace('"', r"\"")
        return f'"{value}"'

    def _to_internal(self) -> _filters._Filter:
        return _filters._AttributeValuePredicate(
            operator=self.operator,
            attribute=self.attribute._to_internal(),
            value=self.value,
        )


@dataclass
class _AttributePredicate(Filter):
    postfix_operator: Literal["EXISTS"]
    attribute: Attribute

    def _to_internal(self) -> _filters._Filter:
        return _filters._AttributePredicate(
            postfix_operator=self.postfix_operator,
            attribute=self.attribute._to_internal(),
        )


@dataclass
class _AssociativeOperator(Filter):
    operator: Literal["AND", "OR"]
    filters: Iterable[Filter]

    def _to_internal(self) -> _filters._Filter:
        return _AlphaInternalAssociativeOperator(
            operator=self.operator,
            filters=[f._to_internal() for f in self.filters],
        )


@dataclass
class _PrefixOperator(Filter):
    operator: Literal["NOT"]
    filter_: Filter

    def _to_internal(self) -> _filters._Filter:
        return _filters._PrefixOperator(
            operator=self.operator,
            filter_=self.filter_._to_internal(),
        )


class _AlphaInternalAssociativeOperator(_filters._AssociativeOperator):
    """
    A version of internal.filters._AssociativeOperator that skips filters validation and allows them to be an empty list
    """

    def __post_init__(self) -> None:
        # Only validate the operator as we're allowing empty filters for _AssociativeOperator in alpha
        allowed_operators = {"AND", "OR"}
        _validate_allowed_value(self.operator, allowed_operators, "operator")
