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
# src/neptune_fetcher/alpha/filters.py
from datetime import datetime
from typing import (
    Literal,
    Optional,
    Union,
)

from neptune_fetcher.internal import filters as _filters
from neptune_fetcher.internal.retrieval import attribute_types as _attribute_types

__all__ = ["Attribute", "AttributeFilter", "Filter"]


ATTRIBUTE_LITERAL = Literal[
    "float", "int", "string", "bool", "datetime", "float_series", "string_set", "string_series", "file"
]
AGGREGATION_LITERAL = Literal["last", "min", "max", "average", "variance"]


def AttributeFilter(
    name_eq: Union[str, list[str], None] = None,
    type_in: Optional[list[ATTRIBUTE_LITERAL]] = None,
    name_matches_all: Union[str, list[str], None] = None,
    name_matches_none: Union[str, list[str], None] = None,
    aggregations: Optional[list[AGGREGATION_LITERAL]] = None,
) -> _filters.AttributeFilter:
    """Filter to apply to attributes when fetching runs or experiments.

    Use to select specific metrics or other metadata based on various criteria.

    Args:
        name_eq (Union[str, list[str], None]): An attribute name or list of names to match exactly.
            If `None`, this filter is not applied.
        type_in (list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set",
        "string_series", "file"]]):
            A list of allowed attribute types. Defaults to all available types.
            For a reference, see: https://docs-beta.neptune.ai/attribute_types
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
    if type_in is None:
        type_in = list(_attribute_types.ALL_TYPES)  # type: ignore
    if aggregations is None:
        aggregations = ["last"]
    return _filters.AttributeFilterMatch(
        name_eq=name_eq,
        type_in=type_in,
        name_matches_all=name_matches_all,
        name_matches_none=name_matches_none,
        aggregations=aggregations,
    )


def Attribute(
    name: str, aggregation: Optional[AGGREGATION_LITERAL] = None, type: Optional[ATTRIBUTE_LITERAL] = None
) -> _filters.Attribute:
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

    return _filters.Attribute(
        name=name,
        aggregation=aggregation,
        type=type,
    )


class Filter:
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
    def eq(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.eq(attribute=attribute, value=value)

    @staticmethod
    def ne(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.ne(attribute=attribute, value=value)

    @staticmethod
    def gt(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.gt(attribute=attribute, value=value)

    @staticmethod
    def ge(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.ge(attribute=attribute, value=value)

    @staticmethod
    def lt(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.lt(attribute=attribute, value=value)

    @staticmethod
    def le(attribute: Union[str, _filters.Attribute], value: Union[int, float, str, datetime]) -> _filters.Filter:
        return _filters.Filter.le(attribute=attribute, value=value)

    @staticmethod
    def matches_all(attribute: Union[str, _filters.Attribute], regex: Union[str, list[str]]) -> _filters.Filter:
        return _filters.Filter.matches_all(attribute=attribute, regex=regex)

    @staticmethod
    def matches_none(attribute: Union[str, _filters.Attribute], regex: Union[str, list[str]]) -> _filters.Filter:
        return _filters.Filter.matches_none(attribute=attribute, regex=regex)

    @staticmethod
    def contains_all(attribute: Union[str, _filters.Attribute], value: Union[str, list[str]]) -> _filters.Filter:
        return _filters.Filter.contains_all(attribute=attribute, value=value)

    @staticmethod
    def contains_none(attribute: Union[str, _filters.Attribute], value: Union[str, list[str]]) -> _filters.Filter:
        return _filters.Filter.contains_none(attribute=attribute, value=value)

    @staticmethod
    def exists(attribute: Union[str, _filters.Attribute]) -> _filters.Filter:
        return _filters.Filter.exists(attribute=attribute)

    @staticmethod
    def all(*filters: _filters.Filter) -> _filters.Filter:
        return _filters.Filter.all(*filters)

    @staticmethod
    def any(*filters: _filters.Filter) -> _filters.Filter:
        return _filters.Filter.any(*filters)

    @staticmethod
    def negate(filter_: _filters.Filter) -> _filters.Filter:
        return _filters.Filter.negate(filter_=filter_)

    @staticmethod
    def name_eq(name: str) -> _filters.Filter:
        return _filters.Filter.name_eq(name=name)

    @staticmethod
    def name_in(*names: str) -> _filters.Filter:
        return _filters.Filter.name_in(*names)
