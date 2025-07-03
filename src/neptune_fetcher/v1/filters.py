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
from neptune_fetcher.internal import pattern as _pattern
from neptune_fetcher.internal.retrieval import attribute_types as types
from neptune_fetcher.internal.util import (
    _validate_allowed_value,
    _validate_list_of_allowed_values,
    _validate_string_or_string_list,
)

__all__ = ["Filter", "AttributeFilter", "Attribute", "KNOWN_TYPES"]


KNOWN_TYPES = frozenset(
    {
        "float",
        "int",
        "string",
        "bool",
        "datetime",
        "float_series",
        "string_set",
        "string_series",
        "file",
        "histogram_series",
    }
)
ALL_AGGREGATIONS = (
    types.FLOAT_SERIES_AGGREGATIONS | types.STRING_SERIES_AGGREGATIONS | types.HISTOGRAM_SERIES_AGGREGATIONS
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
    "histogram_series",
]
AGGREGATION_LITERAL = Literal["last", "min", "max", "average", "variance"]


class BaseAttributeFilter(ABC):
    def __or__(self, other: "BaseAttributeFilter") -> "BaseAttributeFilter":
        return BaseAttributeFilter.any(self, other)

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
        name (str|list[str], optional):
            if str given: an extended regular expression to match attribute names.
            if list[str] given: a list of attribute names to match exactly.
        type (list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set",
        "string_series", "file"]], optional):
            A list of allowed attribute types. Defaults to all available types.
            For a reference, see: https://docs.neptune.ai/attribute_types
        aggregations (list[Literal["last", "min", "max", "average", "variance"]], optional, deprecated): List of
            aggregation functions to apply when fetching metrics of type FloatSeries or StringSeries.
            Defaults to ["last"].

    Example:

    ```
    import neptune_fetcher.v1 as npt
    from neptune_fetcher.v1.filters import AttributeFilter


    loss_avg_and_var = AttributeFilter(
        type=["float_series"],
        name="loss$",
        aggregations=["average", "variance"],
    )

    npt.fetch_experiments_table(attributes=loss_avg_and_var)
    ```
    """

    name: Union[str, list[str], None] = None
    type: list[ATTRIBUTE_LITERAL] = field(default_factory=lambda: list(KNOWN_TYPES))  # type: ignore
    aggregations: list[AGGREGATION_LITERAL] = field(default_factory=lambda: ["last"])

    def __post_init__(self) -> None:
        _validate_string_or_string_list(self.name, "name")
        _validate_list_of_allowed_values(self.type, KNOWN_TYPES, "type")
        _validate_list_of_allowed_values(self.aggregations, ALL_AGGREGATIONS, "aggregations")

    def _to_internal(self) -> _filters._AttributeFilter:
        if isinstance(self.name, str):
            return _pattern.build_extended_regex_attribute_filter(
                self.name,
                type_in=self.type,
                aggregations=self.aggregations,
            )

        if self.name is None:
            return _filters._AttributeFilter(
                type_in=self.type,
                aggregations=self.aggregations,
            )

        if self.name == []:
            raise ValueError(
                "Invalid type for `name` attribute. Expected str, non-empty list of str, or None, but got empty list."
            )

        if isinstance(self.name, list):
            return _filters._AttributeFilter(
                name_eq=self.name,
                type_in=self.type,
                aggregations=self.aggregations,
            )

        raise ValueError(
            "Invalid type for `name` attribute. Expected str, non-empty list of str, or None, but got "
            f"{type(self.name)}."
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
    import neptune_fetcher.v1 as npt
    from neptune_fetcher.v1.filters import Attribute, Filter


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
        _validate_allowed_value(self.type, types.ALL_TYPES, "type")  # type: ignore

    def _to_internal(self) -> _filters._Attribute:
        return _filters._Attribute(
            name=self.name,
            aggregation=self.aggregation,
            type=self.type,
        )

    def __str__(self) -> str:
        return self._to_internal().to_query()


class Filter:
    """Filter used to specify criteria when fetching experiments or runs.

    Examples of filters:
        - Name or attribute value must match regular expression.
        - Attribute value must pass a condition, like "greater than 0.9".
        - Attribute of a given name must exist or not exist.

    You can negate a filter or join multiple filters with logical operators.

    Methods available for attribute values:
    - `name()`: Name of experiment matches an extended regular expression or a list of names.
    - `eq()`: Attribute value equals
    - `ne()`: Attribute value doesn't equal
    - `gt()`: Attribute value is greater than
    - `ge()`: Attribute value is greater than or equal to
    - `lt()`: Attribute value is less than
    - `le()`: Attribute value is less than or equal to
    - `matches()`: Name of experiment matches an extended regular expression
    - `contains_all()`: Tagset contains all tags, or string contains substrings
    - `contains_none()`: Tagset doesn't contain any of the tags, or string doesn't contain the substrings
    - `exists()`: Attribute exists

    Examples:

    ```
    import neptune_fetcher.v1 as npt
    from neptune_fetcher.v1.filters import Filter

    # Fetch metadata from specific experiments
    specific_experiments = Filter.name(["flying-123", "swimming-77"])
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

    def __init__(self, internal: _filters._Filter) -> None:
        self._internal = internal

    @staticmethod
    def name(name: Union[str, list[str]]) -> "Filter":
        name_attribute = Attribute(name="sys/name", type="string")
        if isinstance(name, str):
            return Filter.matches(name_attribute, name)
        else:
            return Filter(_filters._Filter.any([_filters._Filter.eq(name_attribute, n) for n in name]))

    @staticmethod
    def eq(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator="==", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def ne(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator="!=", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def gt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator=">", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def ge(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator=">=", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def lt(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator="<", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def le(attribute: Union[str, Attribute], value: Union[int, float, str, datetime]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributeValuePredicate(operator="<=", attribute=attribute._to_internal(), value=value))

    @staticmethod
    def matches(attribute: Union[str, Attribute], pattern: str) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)

        return Filter(
            _pattern.build_extended_regex_filter(
                attribute=attribute._to_internal(),
                pattern=pattern,
            )
        )

    @staticmethod
    def contains_all(attribute: Union[str, Attribute], values: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)

        if isinstance(values, str):
            values = [values]

        if values == []:
            raise ValueError(
                "Invalid value for `contains_all` filter. Expected str, or non-empty list of str, but got "
                "an empty list"
            )

        internal_filters = [
            _filters._AttributeValuePredicate(operator="CONTAINS", attribute=attribute._to_internal(), value=value)
            for value in values
        ]

        return Filter(_filters._Filter.all(internal_filters))

    @staticmethod
    def contains_none(attribute: Union[str, Attribute], values: Union[str, list[str]]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)

        if values == []:
            raise ValueError(
                "Invalid value for `contains_none` filter. Expected str, or non-empty list of str, but got "
                "an empty list"
            )

        if isinstance(values, str):
            values = [values]

        internal_filters = [
            _filters._AttributeValuePredicate(operator="NOT CONTAINS", attribute=attribute._to_internal(), value=value)
            for value in values
        ]

        return Filter(_filters._Filter.all(internal_filters))

    @staticmethod
    def exists(attribute: Union[str, Attribute]) -> "Filter":
        if isinstance(attribute, str):
            attribute = Attribute(name=attribute)
        return Filter(_filters._AttributePredicate(postfix_operator="EXISTS", attribute=attribute._to_internal()))

    def __and__(self, other: "Filter") -> "Filter":
        """Logical AND operator to combine two filters."""
        return Filter(_filters._Filter.all([self._to_internal(), other._to_internal()]))

    def __or__(self, other: "Filter") -> "Filter":
        """Logical OR operator to combine two filters."""
        return Filter(_filters._Filter.any([self._to_internal(), other._to_internal()]))

    def __invert__(self) -> "Filter":
        """Logical NOT operator to negate the filter."""
        return Filter(_filters._Filter.negate(self._to_internal()))

    def _to_internal(self) -> _filters._Filter:
        return self._internal

    def __str__(self) -> str:
        return self._to_internal().to_query()
