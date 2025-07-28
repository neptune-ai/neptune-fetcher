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
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Iterable,
    Literal,
    Sequence,
    Union,
)

from neptune_query.internal import filters as _filters
from neptune_query.internal import pattern as _pattern
from neptune_query.internal.retrieval import attribute_types as types
from neptune_query.internal.util import (
    _validate_allowed_value,
    _validate_list_of_allowed_values,
    _validate_string_or_string_list,
)

__all__ = ["Filter", "AttributeFilter", "Attribute", "KNOWN_TYPES"]


# fmt: off
KNOWN_TYPES_LITERAL = Literal["bool", "datetime", "file", "float", "int", "string", "string_set", "float_series", "histogram_series", "string_series", "file_series"]  # noqa: E501
# fmt: on

KNOWN_TYPES: Sequence[KNOWN_TYPES_LITERAL] = (
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
    "file_series",
)
AGGREGATION_LAST: Sequence[Literal["last"]] = ("last",)


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
    """Specifies criteria for attributes when using a fetching method.

    Args:
        name: Criterion for attribute names.
            If a string is provided, it's treated as a regex pattern that the attribute name must match.
            Supports Neptune's extended regex syntax.
            If a list of strings is provided, it's treated as exact attribute names to match.
        type: List of allowed attribute types (or a single type as str). Defaults to all available types:
            ["float", "int", "string", "bool", "datetime", "float_series", "string_set", "string_series",
            "file", "histogram_series"]
            For reference, see: https://docs.neptune.ai/attribute_types

    Example:

    From a particular experiment, fetch values from all FloatSeries attributes with "loss" in the name:
    ```
    import neptune_query as nq
    from neptune_query.filters import AttributeFilter


    losses = AttributeFilter(name=r"loss", type="float_series")
    loss_values = nq.fetch_metrics(
        experiments=["training-week-34"],
        attributes=losses,
    )
    ```
    """

    # Stopping formatting here to allow long Literal lines
    # fmt: off
    name: Union[str, list[str], None] = None
    type: Union[
        Literal["bool", "datetime", "file", "float", "int", "string", "string_set", "float_series", "histogram_series", "string_series", "file_series"],  # noqa: E501
        list[
            Literal["bool", "datetime", "file", "float", "int", "string", "string_set", "float_series", "histogram_series", "string_series", "file_series"],  # noqa: E501
        ],
        None,
    ] = None
    # fmt: on

    def __post_init__(self) -> None:
        if self.type is None:
            self.type = list(KNOWN_TYPES)
        elif isinstance(self.type, str):
            self.type = [self.type]
        _validate_string_or_string_list(self.name, "name")
        _validate_list_of_allowed_values(self.type, KNOWN_TYPES, "type")

    def _to_internal(self) -> _filters._BaseAttributeFilter:
        types: Sequence[KNOWN_TYPES_LITERAL] = self.type  # type: ignore  # it's converted into seq in __post_init__

        if isinstance(self.name, str):
            return _pattern.build_extended_regex_attribute_filter(
                self.name,
                type_in=types,
                aggregations=AGGREGATION_LAST,
            )

        if self.name is None:
            return _filters._AttributeFilter(
                type_in=types,
                aggregations=AGGREGATION_LAST,
            )

        if isinstance(self.name, list):
            return _filters._AttributeFilter(
                name_eq=self.name,
                type_in=types,
                aggregations=AGGREGATION_LAST,
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
    """Specifies an attribute and its type.

    When fetching experiments or runs, use this class to filter and sort the returned entries.

    Args:
        name: Attribute name to match exactly.
        type: Attribute type. Specify it to resolve ambiguity, in case some of the project's runs contain attributes
            that have the same name but are of a different type. Available types:
            ["bool", "datetime", "file", "float", "int", "string", "string_set", "float_series", "histogram_series",
            "string_series", "file_series"].
            For a reference, see: https://docs.neptune.ai/attribute_types

    Example:
        Fetch metadata from experiments with "config/batch_size" set to the integer 64:
        ```
        import neptune_query as nq
        from neptune_query.filters import Attribute, Filter


        batch_size = Attribute(
            name="config/batch_size",
            type="int",
        )
        batch_size_64 = Filter.eq(batch_size, 64)
        nq.fetch_experiments_table(experiments=batch_size_64)
        ```
    """

    # fmt: off
    name: str
    type: Union[
        Literal["bool", "datetime", "file", "float", "int", "string", "string_set", "float_series", "histogram_series", "string_series", "file_series"],  # noqa: E501
        None,
    ] = None
    # fmt: on

    def __post_init__(self) -> None:
        _validate_allowed_value(self.type, types.ALL_TYPES, "type")

    def _to_internal(self) -> _filters._Attribute:
        return _filters._Attribute(
            name=self.name,
            type=self.type,
        )

    def __str__(self) -> str:
        return self._to_internal().to_query()


class Filter:
    """Specifies criteria for experiments or attributes when using a fetching method.

    For regular expressions, the extended syntax is supported.
    For details, see https://docs.neptune.ai/regex/#compound-expressions

    Examples of filters:
        - Name or attribute value must match a regex.
        - Attribute value must pass a condition, like "greater than 0.9".
        - Attribute of a given name must exist.

    You can negate a filter or join multiple filters with logical operators.

    Methods available for attribute values:
        - `name()`: Experiment name matches a regex or a list of names
        - `eq()`: Attribute value equals
        - `ne()`: Attribute value doesn't equal
        - `gt()`: Attribute value is greater than
        - `ge()`: Attribute value is greater than or equal to
        - `lt()`: Attribute value is less than
        - `le()`: Attribute value is less than or equal to
        - `matches()`: String attribute value matches a regex
        - `contains_all()`: Tagset contains all tags, or string contains substrings
        - `contains_none()`: Tagset doesn't contain any of the tags, or string doesn't contain the substrings
        - `exists()`: Attribute exists

    Examples:
        Fetch loss values from experiments with specific tags:
        ```
        import neptune_query as nq
        from neptune_query.filters import Filter


        specific_tags = Filter.contains_all("sys/tags", ["fly", "swim", "nest"])
        nq.fetch_metrics(experiments=specific_tags, attributes=r"^metrics/loss/")
        ```

        List my experiments that have a "dataset_version" attribute and "validation/loss" less than 0.1:
        ```
        owned_by_me = Filter.eq("sys/owner", "sigurd")
        dataset_check = Filter.exists("dataset_version")
        loss_filter = Filter.lt("validation/loss", 0.1)

        interesting = owned_by_me & dataset_check & loss_filter
        nq.list_experiments(experiments=interesting)
        ```

        Fetch configs from the interesting experiments:
        ```
        nq.fetch_experiments_table(experiments=interesting, attributes=r"config/")
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
            return Filter(_filters._Filter.any([_filters._Filter.eq(name_attribute._to_internal(), n) for n in name]))

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
