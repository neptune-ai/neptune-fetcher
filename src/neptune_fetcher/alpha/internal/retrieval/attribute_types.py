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
from dataclasses import dataclass
from typing import (
    Any,
    Optional,
)

from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoFloatSeriesAttributeDTO,
)

from neptune_fetcher.alpha.exceptions import warn_unsupported_value_type

ALL_TYPES = ("float", "int", "string", "bool", "datetime", "float_series", "string_set")
ALL_AGGREGATIONS = {"last", "min", "max", "average", "variance"}

_ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP = {
    "float_series": "floatSeries",
    "string_set": "stringSet",
}

_ATTRIBUTE_TYPE_BACKEND_TO_PYTHON_MAP = {v: k for k, v in _ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP.items()}


def map_attribute_type_python_to_backend(_type: str) -> str:
    return _ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP.get(_type, _type)


def map_attribute_type_backend_to_python(_type: str) -> str:
    return _ATTRIBUTE_TYPE_BACKEND_TO_PYTHON_MAP.get(_type, _type)


@dataclass(frozen=True)
class FloatSeriesAggregations:
    last: float
    min: float
    max: float
    average: float
    variance: float


def extract_value(attr: ProtoAttributeDTO) -> Optional[Any]:
    if attr.type == "floatSeries":
        return _extract_aggregations(attr.float_series_properties)
    elif attr.type == "string":
        return attr.string_properties.value
    elif attr.type == "int":
        return attr.int_properties.value
    elif attr.type == "float":
        return attr.float_properties.value
    elif attr.type == "bool":
        return attr.bool_properties.value
    elif attr.type == "datetime":
        return datetime.datetime.fromtimestamp(attr.datetime_properties.value / 1000, tz=datetime.timezone.utc)
    elif attr.type == "stringSet":
        return set(attr.string_set_properties.value)
    elif attr.type == "experimentState":
        return None
    else:
        warn_unsupported_value_type(attr.type)
        return None


def _extract_aggregations(attr: ProtoFloatSeriesAttributeDTO) -> FloatSeriesAggregations:
    return FloatSeriesAggregations(
        last=attr.last,
        min=attr.min,
        max=attr.max,
        average=attr.average,
        variance=attr.variance,
    )
