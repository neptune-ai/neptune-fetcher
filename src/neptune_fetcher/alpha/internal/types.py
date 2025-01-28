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
import datetime
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    Literal,
    Optional,
    TypeVar,
)

from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoFloatSeriesAttributeDTO,
)

from neptune_fetcher.alpha.internal.exception import NeptuneWarning

ALL_TYPES = ("float", "int", "string", "bool", "datetime", "float_series", "string_set")

_ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP = {
    "float_series": "floatSeries",
    "string_set": "stringSet",
}

_ATTRIBUTE_TYPE_BACKEND_TO_PYTHON_MAP = {v: k for k, v in _ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP.items()}


def map_attribute_type_python_to_backend(_type: str) -> str:
    return _ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP.get(_type, _type)


def map_attribute_type_backend_to_python(_type: str) -> str:
    return _ATTRIBUTE_TYPE_BACKEND_TO_PYTHON_MAP.get(_type, _type)


T = TypeVar("T")


@dataclass(frozen=True)
class AttributeValue(Generic[T]):
    name: str
    type: Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set"]
    value: T


@dataclass(frozen=True)
class FloatSeriesAggregates:
    last: float
    min: float
    max: float
    average: float
    variance: float


def extract_value(attr: ProtoAttributeDTO) -> Optional[AttributeValue[Any]]:
    if attr.type == "floatSeries":
        return AttributeValue(attr.name, "float_series", extract_aggregates(attr.float_series_properties))
    elif attr.type == "string":
        return AttributeValue(attr.name, "string", attr.string_properties.value)
    elif attr.type == "int":
        return AttributeValue(attr.name, "int", attr.int_properties.value)
    elif attr.type == "float":
        return AttributeValue(attr.name, "float", attr.float_properties.value)
    elif attr.type == "bool":
        return AttributeValue(attr.name, "bool", attr.bool_properties.value)
    elif attr.type == "datetime":
        timestamp = datetime.datetime.fromtimestamp(attr.datetime_properties.value / 1000, tz=datetime.timezone.utc)
        return AttributeValue(attr.name, "datetime", timestamp)
    elif attr.type == "stringSet":
        return AttributeValue(attr.name, "string_set", set(attr.string_set_properties.value))
    elif attr.type == "experimentState":
        return None
    else:
        warn_unsupported_value_type(attr.type)
        return None


def extract_aggregates(attr: ProtoFloatSeriesAttributeDTO) -> FloatSeriesAggregates:
    return FloatSeriesAggregates(
        last=attr.last,
        min=attr.min,
        max=attr.max,
        average=attr.average,
        variance=attr.variance,
    )


# We keep a set of types we've warned the user about to make sure we warn about a type only once.
# This is necessary because of a bug in pandas, that causes duplicate warnings to be issued everytime after an
# DataFrame() is created (presumably only empty DF).
# The bug basically makes `warnings.simplefilter("once", NeptuneWarning)` not work as expected, and would flood
# the user with warnings in some cases.
_warned_types = set()


def warn_unsupported_value_type(type_: str) -> None:
    if type_ in _warned_types:
        return

    _warned_types.add(type_)
    warnings.warn(
        f"A value of type `{type_}` was returned by your query. This type is not supported by your installed version "
        "of neptune-fetcher. Values will evaluate to `None` and empty DataFrames. "
        "Upgrade neptune-fetcher to access this data.",
        NeptuneWarning,
    )
