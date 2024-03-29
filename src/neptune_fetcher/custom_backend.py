#
# Copyright (c) 2024, Neptune Labs Sp. z o.o.
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
#

from typing import Any

from neptune.internal.utils.logger import get_logger

from neptune_fetcher.attribute_type import AttributeType
from neptune_fetcher.attributes import (
    Attr,
    Float,
    Integer,
    String,
)

logger = get_logger()


def get_attribute_from_dto(dto: Any) -> Attr:
    if dto.stringProperties is not None:
        return String(AttributeType(dto.type), dto.stringProperties.value)
    if dto.floatProperties is not None:
        return Float(AttributeType(dto.type), dto.floatProperties.value)
    if dto.intProperties is not None:
        return Integer(AttributeType(dto.type), dto.intProperties.value)
    raise Exception(f"Field {dto.name} of type {AttributeType(dto.type)} does not support prefetching")
