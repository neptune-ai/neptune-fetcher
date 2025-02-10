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

from neptune_fetcher.alpha.internal.retrieval import attribute_types as types

__all__ = ["Filter", "AttributeFilter", "Attribute"]


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return BaseAttributeFilter.any(self, other)

    def any(*filters: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return _AttributeFilterAlternative(filters=filters)


@dataclass
class AttributeFilter(BaseAttributeFilter):
    """Filter to apply to attributes when fetching runs or experiments.

    Use to select specific metrics or other metadata based on various criteria.

    Args:
        name_eq (Union[str, list[str], None]): An attribute name or list of names to match exactly.
            If `None`, this filter is not applied.
        type_in (list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]]):
            A list of allowed attribute types. Defaults to all available types.
            For a reference, see: https://docs-beta.neptune.ai/attribute_types
        name_matches_all (Union[str, list[str], None]): A regular expression or list of expressions that the attribute
            name must match. If `None`, this filter is not applied.
        name_matches_none (Union[str, list[str], None]): A regular expression or list of expressions that the attribute
            names mustn't match. Attributes matching any of the regexes are excluded.
            If `None`, this filter is not applied.
        aggregations (list[Literal["last", "min", "max", "average", "variance"]]): List of aggregation functions
            to apply when fetching metrics of type FloatSeries. Defaults to ["last"].

    Examples:

    Import the needed classes:

    ```
    import neptune_fetcher.alpha as npt
    from npt.filters import AttributeFilter
    ```

    Select attribute by exact name:

    ```
    AttributeFilter(name_eq="config/optimizer")
    ```

    Select metrics not matching regexes `^test` or `loss$` and pick the "average" and "variance" aggregations:

    ```
    AttributeFilter(
        type_in=["float_series"],
        name_matches_none=[r"^test", r"loss$"],
        aggregations=["average", "variance"],
    )
    ```

    Combine multiple filters with the pipe character:

    ```
    filter_1 = AttributeFilter(...)
    filter_2 = AttributeFilter(...)
    filter_3 = AttributeFilter(...)
    alternatives = filter_1 | filter_2 | filter_3
    ```
    """

    name_eq: Union[str, list[str], None] = None
    type_in: list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]] = field(
        default_factory=lambda: list(types.ALL_TYPES)  # type: ignore
    )
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: list[Literal["last", "min", "max", "average", "variance"]] = field(default_factory=lambda: ["last"])

    def __post_init__(self) -> None:
        _validate_string_or_string_list(self.name_eq, "name_eq")
        _validate_string_or_string_list(self.name_matches_all, "name_matches_all")
        _validate_string_or_string_list(self.name_matches_none, "name_matches_none")

        _validate_list_of_allowed_values(self.type_in, types.ALL_TYPES, "type_in")  # type: ignore
        _validate_list_of_allowed_values(self.aggregations, types.ALL_AGGREGATIONS, "aggregations")  # type: ignore


@dataclass
class _AttributeFilterAlternative(BaseAttributeFilter):
    filters: Iterable[BaseAttributeFilter]

    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        filters = tuple(self.filters) + (other,)
        return _AttributeFilterAlternative(filters)


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
            For a reference, see: https://docs-beta.neptune.ai/attribute_types

    Examples:

    Select a metric and pick variance as the aggregation:

    ```
    import neptune_fetcher.alpha as npt
    from npt.filters import Attribute, Filter


    val_loss_variance = Attribute(
        name="val/loss",
        aggregation="variance",
    )
    ```

    Construct a filter around the attribute, then pass it to a fetching or listing method:

    ```
    tiny_val_loss_variance = Filter.lt(val_loss_variance, 0.01)
    npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
    ```
    """

    name: str
    aggregation: Optional[Literal["last", "min", "max", "average", "variance"]] = None
    type: Optional[Literal["bool", "int", "float", "string", "datetime", "float_series", "string_set"]] = None

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


class Filter(ABC):
    """Filter used to specify criteria when fetching experiments or runs.

    Examples of filters:
        - Name or attribute must match regular expression.
        - Attribute value must pass a condition, like "greater than 0.9".

    You can negate a filter or join multiple filters with logical operators.

    Methods available for attribute values:
    - `eq()`: Equals
    - `ne()`: Doesn't equal
    - `gt()`: Greater than
    - `ge()`: Greater than or equal to
    - `lt()`: Less than
    - `le()`: Less than or equal to
    - `matches_all()`: Matches regex or all in list of regexes
    - `matches_none()`: Doesn't match regex or any of list of regexes
    - `contains_all()`: Tagset contains all tags, or string contains substrings
    - `contains_none()`: Tagset doesn't contain any of the tags, or string doesn't contain the substrings
    - `exists()`: Attribute exists

    Examples:

    Import the needed classes:

    ```
    import neptune_fetcher.alpha as npt
    from npt.filters import Attribute, Filter
    ```

    Constructing filters:

    A) Regex that the experiment or run name must match:

    ```
    name_filter = Filter.matches_all("sys/name", r"kittiwake$")
    ```

    B) Don't allow the tags "test" or "val":

    ```
    no_test_or_val = Filter.contains_none("sys/tags", ["test", "val"])
    ```

    C) Set a condition for the last logged value of a metric:

    ```
    loss_filter = Filter.lt("validation/loss", 0.1)
    ```

    For more control over the selected metric, use the `Attribute()` helper class.

    D) Negate a filter: Call `negate()` or prepend with `~`:

    ```
    not_loss_filter = ~loss_filter
    # equivalent to
    not_loss_filter = Filter.negate(loss_filter)
    ```

    C) Combining filters:

    - To join with AND: Use `&` or pass the filters to the `all()` method.
    - To join with OR: Use `|` or pass the filters to the `any()` method.

    ```
    name_and_loss_filter = name_filter & loss_filter
    # equivalent to
    name_and_loss_filter = Filter.all(name_filter, loss_filter)
    ```

    To use a filter in a query, pass it as the argument to a fetching or listing method:

    ```
    npt.fetch_experiments_table(experiments=name_and_loss_filter)
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

    @abc.abstractmethod
    def to_query(self) -> str:
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

    def to_query(self) -> str:
        return f"{self.attribute} {self.operator} {self._right_query()}"

    def _right_query(self) -> str:
        if isinstance(self.value, datetime):
            return f'"{self.value.astimezone().isoformat()}"'
        value = str(self.value)
        value = value.replace("\\", r"\\").replace('"', r"\"")
        return f'"{value}"'


@dataclass
class _AttributePredicate(Filter):
    postfix_operator: Literal["EXISTS"]
    attribute: Attribute

    def to_query(self) -> str:
        return f"{self.attribute} {self.postfix_operator}"


@dataclass
class _AssociativeOperator(Filter):
    operator: Literal["AND", "OR"]
    filters: Iterable[Filter]

    def to_query(self) -> str:
        filter_queries = [f"({child})" for child in self.filters]
        return f" {self.operator} ".join(filter_queries)


@dataclass
class _PrefixOperator(Filter):
    operator: Literal["NOT"]
    filter_: Filter

    def to_query(self) -> str:
        return f"{self.operator} ({self.filter_})"


def _validate_string_or_string_list(value: Optional[Union[str, list[str]]], field_name: str) -> None:
    """Validate that a value is either None, a string, or a list of strings."""
    if value is not None:
        if isinstance(value, list):
            if not all(isinstance(item, str) for item in value):
                raise ValueError(f"{field_name} must be a string or list of strings")
        elif not isinstance(value, str):
            raise ValueError(f"{field_name} must be a string or list of strings")


def _validate_list_of_allowed_values(value: list[str], allowed_values: set[str], field_name: str) -> None:
    """Validate that a value is a list containing only allowed values."""
    if not isinstance(value, list) or not all(isinstance(v, str) and v in allowed_values for v in value):
        raise ValueError(f"{field_name} must be a list of valid values: {sorted(allowed_values)}")


def _validate_allowed_value(value: Optional[str], allowed_values: set[str], field_name: str) -> None:
    """Validate that a value is either None or one of the allowed values."""
    if value is not None and (not isinstance(value, str) or value not in allowed_values):
        raise ValueError(f"{field_name} must be one of: {sorted(allowed_values)}")
