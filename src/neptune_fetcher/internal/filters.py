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
    Callable,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Union,
)

from neptune_fetcher.internal.retrieval import attribute_types as types

__all__ = ["_Filter", "_AttributeFilter", "_Attribute"]

from neptune_fetcher.internal.util import (
    _validate_allowed_value,
    _validate_list_of_allowed_values,
    _validate_string_list,
    _validate_string_or_string_list,
)

ATTRIBUTE_LITERAL = Literal[
    "float",
    "int",
    "string",
    "bool",
    "datetime",
    "float_series",
    "string_set",
    "string_series",
    "file",
    "file_series",
    "histogram_series",
]
AGGREGATION_LITERAL = Literal["last", "min", "max", "average", "variance"]


class _BaseAttributeFilter(ABC):
    @staticmethod
    def any(filters: list["_BaseAttributeFilter"]) -> "_BaseAttributeFilter":
        return _AttributeFilterAlternative(filters=filters)

    @abc.abstractmethod
    def transform(
        self, map_attribute_filter: Callable[["_AttributeFilter"], "_AttributeFilter"]
    ) -> "_BaseAttributeFilter":
        ...


@dataclass
class _AttributeNameFilter:
    must_match_regexes: Optional[list[str]] = None
    must_not_match_regexes: Optional[list[str]] = None


@dataclass
class _AttributeFilter(_BaseAttributeFilter):
    """Filter to apply to attributes when fetching runs or experiments.

    Use to select specific metrics or other metadata based on various criteria.

    Args:
        name_eq (Union[str, list[str], None]): An attribute name or list of names to match exactly.
            If `None`, this filter is not applied.
        type_in (list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set",
        "string_series", "file"]]):
            A list of allowed attribute types. Defaults to all available types.
            For a reference, see: https://docs.neptune.ai/attribute_types
        must_match_any (Optional[list[_AttributeNameFilter]]):
            If `None`, this filter is not applied. Otherwise, it's a list of `_AttributeNameFilter` instances.
            Each instance specifies a set of regexes that the attribute name must match or not match.
            Any one of the list items must match for the attribute to be included.
            Each list item should be an `_AttributeNameFilter` and it can contain:
                name_matches_all (Union[str, list[str], None]): A list of regular expressions that the attribute
                    name must match. If `None`, this filter is not applied.
                name_matches_none (Union[str, list[str], None]): A list of regular expressions that the attribute
                    names mustn't match. Attributes matching any of the regexes are excluded.
                    If `None`, this filter is not applied.
        aggregations (list[Literal["last", "min", "max", "average", "variance"]]): List of
            aggregation functions to apply when fetching metrics of type FloatSeries or StringSeries.
            Defaults to ["last"].

    Example:

    ```
    from neptune_fetcher.internal.filters import _AttributeFilter


    loss_avg_and_var = _AttributeFilter(
        type_in=["float_series"],
        name_matches_all=[r"loss$"],
        aggregations=["average", "variance"],
    )

    npt.fetch_experiments_table(attributes=loss_avg_and_var)
    ```
    """

    name_eq: Union[str, list[str], None] = None
    type_in: Sequence[ATTRIBUTE_LITERAL] = field(default_factory=lambda: list(types.ALL_TYPES))
    must_match_any: Optional[list[_AttributeNameFilter]] = None
    aggregations: Sequence[AGGREGATION_LITERAL] = field(default_factory=lambda: ["last"])

    def __post_init__(self) -> None:
        _validate_string_or_string_list(self.name_eq, "name_eq")

        if self.must_match_any is not None:
            if not isinstance(self.must_match_any, list):
                raise ValueError("must_match_any must be a list of _AttributeNameFilter instances")
            for item in self.must_match_any:
                if not isinstance(item, _AttributeNameFilter):
                    raise ValueError("must_match_any must contain only _AttributeNameFilter instances")
                _validate_string_list(item.must_match_regexes, "must_match_regexes")
                _validate_string_list(item.must_not_match_regexes, "must_not_match_regexes")

        _validate_list_of_allowed_values(self.type_in, types.ALL_TYPES, "type_in")
        _validate_list_of_allowed_values(self.aggregations, types.ALL_AGGREGATIONS, "aggregations")

    def transform(
        self, map_attribute_filter: Callable[["_AttributeFilter"], "_AttributeFilter"]
    ) -> "_BaseAttributeFilter":
        return map_attribute_filter(self)


@dataclass
class _AttributeFilterAlternative(_BaseAttributeFilter):
    filters: Iterable[_BaseAttributeFilter]

    def __or__(self, other: "_BaseAttributeFilter") -> "_BaseAttributeFilter":
        filters = tuple(self.filters) + (other,)
        return _AttributeFilterAlternative(filters)

    def transform(
        self, map_attribute_filter: Callable[["_AttributeFilter"], "_AttributeFilter"]
    ) -> "_BaseAttributeFilter":
        transformed_filters = [f.transform(map_attribute_filter) for f in self.filters]
        return _AttributeFilterAlternative(transformed_filters)


@dataclass
class _Attribute:
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
    from neptune_fetcher.internal.filters import _Attribute, _Filter


    val_loss_variance = _Attribute(
        name="val/loss",
        aggregation="variance",
    )
    # Construct a filter and pass it to a fetching or listing method
    tiny_val_loss_variance = _Filter.lt(val_loss_variance, 0.01)
    npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
    ```
    """

    name: str
    aggregation: Optional[AGGREGATION_LITERAL] = None
    type: Optional[ATTRIBUTE_LITERAL] = None

    def __post_init__(self) -> None:
        _validate_allowed_value(self.aggregation, types.ALL_AGGREGATIONS, "aggregation")
        _validate_allowed_value(self.type, types.ALL_TYPES, "type")  # type: ignore

    def to_query(self) -> str:
        query = f"`{self.name}`"

        if self.type is not None:
            _type = types.map_attribute_type_python_to_backend(self.type)
            query += f":{_type}"

        if self.aggregation is not None:
            query = f"{self.aggregation}({query})"

        return query

    def __str__(self) -> str:
        return self.to_query()


class _Filter(ABC):
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
    from neptune_fetcher.internal.filters import _Filter

    # Fetch metadata from specific experiments
    specific_experiments = _Filter.name_in("flying-123", "swimming-77")
    npt.fetch_experiments_table(experiments=specific_experiments)

    # Define various criteria
    owned_by_me = _Filter.eq("sys/owner", "vidar")
    loss_filter = _Filter.lt("validation/loss", 0.1)
    tag_filter = _Filter.contains_none("sys/tags", ["test", "buggy"])
    dataset_check = _Filter.exists("dataset_version")

    my_interesting_experiments = owned_by_me & loss_filter & tag_filter & dataset_check
    npt.fetch_experiments_table(experiments=my_interesting_experiments)
    ```
    """

    @staticmethod
    def eq(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator="==", attribute=attribute, value=value)

    @staticmethod
    def ne(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator="!=", attribute=attribute, value=value)

    @staticmethod
    def gt(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator=">", attribute=attribute, value=value)

    @staticmethod
    def ge(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator=">=", attribute=attribute, value=value)

    @staticmethod
    def lt(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator="<", attribute=attribute, value=value)

    @staticmethod
    def le(attribute: Union[str, _Attribute], value: Union[int, float, str, datetime]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributeValuePredicate(operator="<=", attribute=attribute, value=value)

    @staticmethod
    def matches_all(attribute: Union[str, _Attribute], regex: Union[str, list[str]]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="MATCHES", attribute=attribute, value=regex)
        else:
            filters = [_Filter.matches_all(attribute, r) for r in regex]
            return _Filter.all(filters)

    @staticmethod
    def matches_none(attribute: Union[str, _Attribute], regex: Union[str, list[str]]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        if isinstance(regex, str):
            return _AttributeValuePredicate(operator="NOT MATCHES", attribute=attribute, value=regex)
        else:
            filters = [_Filter.matches_none(attribute, r) for r in regex]
            return _Filter.all(filters)

    @staticmethod
    def contains_all(attribute: Union[str, _Attribute], value: Union[str, list[str]]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="CONTAINS", attribute=attribute, value=value)
        else:
            filters = [_Filter.contains_all(attribute, v) for v in value]
            return _Filter.all(filters)

    @staticmethod
    def contains_none(attribute: Union[str, _Attribute], value: Union[str, list[str]]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        if isinstance(value, str):
            return _AttributeValuePredicate(operator="NOT CONTAINS", attribute=attribute, value=value)
        else:
            filters = [_Filter.contains_none(attribute, v) for v in value]
            return _Filter.all(filters)

    @staticmethod
    def exists(attribute: Union[str, _Attribute]) -> "_Filter":
        if isinstance(attribute, str):
            attribute = _Attribute(name=attribute)
        return _AttributePredicate(postfix_operator="EXISTS", attribute=attribute)

    @staticmethod
    def all(filters: list["_Filter"]) -> "_Filter":
        """Combine multiple filters with a logical AND operator."""
        if not filters or not isinstance(filters, list):
            raise ValueError("At least one filter must be provided to combine with AND.")
        return _AssociativeOperator(operator="AND", filters=filters)

    @staticmethod
    def any(filters: list["_Filter"]) -> "_Filter":
        """Combine multiple filters with a logical OR operator."""
        if not filters or not isinstance(filters, list):
            raise ValueError("At least one filter must be provided to combine with OR.")
        return _AssociativeOperator(operator="OR", filters=filters)

    @staticmethod
    def negate(filter_: "_Filter") -> "_Filter":
        return _PrefixOperator(operator="NOT", filter_=filter_)

    @staticmethod
    def name_eq(name: str) -> "_Filter":
        name_attribute = _Attribute(name="sys/name", type="string")
        return _Filter.eq(name_attribute, name)

    @abc.abstractmethod
    def to_query(self) -> str:
        ...

    def __str__(self) -> str:
        return self.to_query()


@dataclass
class _AttributeValuePredicate(_Filter):
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "MATCHES", "NOT MATCHES", "CONTAINS", "NOT CONTAINS"]
    attribute: _Attribute
    value: Union[bool, int, float, str, datetime]

    def __post_init__(self) -> None:
        allowed_operators = {"==", "!=", ">", ">=", "<", "<=", "MATCHES", "NOT MATCHES", "CONTAINS", "NOT CONTAINS"}
        _validate_allowed_value(self.operator, allowed_operators, "operator")

        if not isinstance(self.value, (bool, int, float, str, datetime)):
            raise TypeError(f"Invalid value type: {type(self.value).__name__}. Expected int, float, str, or datetime.")

    def to_query(self) -> str:
        return f"{self.attribute} {self.operator} {self._right_query()}"

    def _right_query(self) -> str:
        if isinstance(self.value, datetime):
            return f'"{self.value.astimezone().isoformat()}"'
        value = str(self.value)
        value = value.replace("\\", r"\\").replace('"', r"\"")
        return f'"{value}"'


@dataclass
class _AttributePredicate(_Filter):
    postfix_operator: Literal["EXISTS"]
    attribute: _Attribute

    def to_query(self) -> str:
        return f"{self.attribute} {self.postfix_operator}"


@dataclass
class _AssociativeOperator(_Filter):
    operator: Literal["AND", "OR"]
    filters: Iterable[_Filter]

    def __post_init__(self) -> None:
        allowed_operators = {"AND", "OR"}
        _validate_allowed_value(self.operator, allowed_operators, "operator")
        if not self.filters:
            raise ValueError(f"At least one filter must be provided to combine with {self.operator}.")

    def to_query(self) -> str:
        filter_queries = [f"({child})" for child in self.filters]
        return f" {self.operator} ".join(filter_queries)


@dataclass
class _PrefixOperator(_Filter):
    operator: Literal["NOT"]
    filter_: _Filter

    def to_query(self) -> str:
        return f"{self.operator} ({self.filter_})"
