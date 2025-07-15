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
    Literal,
    Optional,
    Sequence,
)

from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoFileRefAttributeDTO,
    ProtoFileRefSeriesAttributeDTO,
    ProtoFloatSeriesAttributeDTO,
    ProtoHistogramSeriesAttributeDTO,
    ProtoStringSeriesAttributeDTO,
)

from ...exceptions import warn_unsupported_value_type

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
ALL_TYPES: Sequence[ATTRIBUTE_LITERAL] = (
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
)
FLOAT_SERIES_AGGREGATIONS = frozenset({"last", "min", "max", "average", "variance"})
STRING_SERIES_AGGREGATIONS = frozenset({"last"})
FILE_SERIES_AGGREGATIONS = frozenset({"last"})
HISTOGRAM_SERIES_AGGREGATIONS = frozenset({"last"})
ALL_AGGREGATIONS = (
    FLOAT_SERIES_AGGREGATIONS | STRING_SERIES_AGGREGATIONS | FILE_SERIES_AGGREGATIONS | HISTOGRAM_SERIES_AGGREGATIONS
)
TYPE_AGGREGATIONS = {
    "float_series": FLOAT_SERIES_AGGREGATIONS,
    "string_series": STRING_SERIES_AGGREGATIONS,
    "file_series": FILE_SERIES_AGGREGATIONS,
    "histogram_series": HISTOGRAM_SERIES_AGGREGATIONS,
}

_ATTRIBUTE_TYPE_PYTHON_TO_BACKEND_MAP = {
    "float_series": "floatSeries",
    "string_set": "stringSet",
    "string_series": "stringSeries",
    "file": "fileRef",
    "file_series": "fileRefSeries",
    "histogram_series": "histogramSeries",
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


@dataclass(frozen=True)
class File:
    path: str
    size_bytes: int
    mime_type: str

    def __repr__(self) -> str:
        return f"File({self.mime_type}, size={humanize_size(self.size_bytes)})"


@dataclass(frozen=True)
class Histogram:
    type: str
    edges: list[float]
    values: list[float]


@dataclass(frozen=True)
class StringSeriesAggregations:
    last: Optional[str]
    last_step: Optional[float]


@dataclass(frozen=True)
class FileSeriesAggregations:
    last: Optional[File]
    last_step: Optional[float]


@dataclass(frozen=True)
class HistogramSeriesAggregations:
    last: Optional[Histogram]
    last_step: Optional[float]


def humanize_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"


def extract_value(attr: ProtoAttributeDTO) -> Optional[Any]:
    if attr.type == "floatSeries":
        return _extract_float_series_aggregations(attr.float_series_properties)
    elif attr.type == "stringSeries":
        return _extract_string_series_aggregations(attr.string_series_properties)
    elif attr.type == "fileRefSeries":
        return _extract_file_ref_series_aggregations(attr.file_ref_series_properties)
    elif attr.type == "histogramSeries":
        return _extract_histogram_series_aggregations(attr.histogram_series_properties)
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
    elif attr.type == "fileRef":
        return _extract_file_ref_properties(attr.file_ref_properties)
    elif attr.type == "experimentState":
        return None
    else:
        warn_unsupported_value_type(attr.type)
        return None


def _extract_float_series_aggregations(attr: ProtoFloatSeriesAttributeDTO) -> FloatSeriesAggregations:
    return FloatSeriesAggregations(
        last=attr.last,
        min=attr.min,
        max=attr.max,
        average=attr.average,
        variance=attr.variance,
    )


def _extract_file_ref_properties(attr: ProtoFileRefAttributeDTO) -> File:
    return File(
        path=attr.path,
        size_bytes=attr.sizeBytes,
        mime_type=attr.mimeType,
    )


def _extract_string_series_aggregations(attr: ProtoStringSeriesAttributeDTO) -> StringSeriesAggregations:
    return StringSeriesAggregations(
        last=attr.last,
        last_step=attr.last_step,
    )


def _extract_file_ref_series_aggregations(attr: ProtoFileRefSeriesAttributeDTO) -> FileSeriesAggregations:
    return FileSeriesAggregations(
        last=File(
            path=attr.last.path,
            size_bytes=attr.last.sizeBytes,
            mime_type=attr.last.mimeType,
        ),
        last_step=attr.last_step,
    )


def _extract_histogram_series_aggregations(attr: ProtoHistogramSeriesAttributeDTO) -> HistogramSeriesAggregations:
    return HistogramSeriesAggregations(
        last=Histogram(
            type=attr.last.type,
            edges=list(attr.last.edges),
            values=list(attr.last.values),
        ),
        last_step=attr.last_step,
    )
