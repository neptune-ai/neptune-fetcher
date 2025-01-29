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


ALL_TYPES = ("float", "int", "string", "bool", "datetime", "float_series", "string_set")

_ATTRIBUTE_TYPE_USER_TO_BACKEND_MAP = {
    "float_series": "floatSeries",
    "string_set": "stringSet",
}

_ATTRIBUTE_TYPE_BACKEND_TO_USER_MAP = {v: k for k, v in _ATTRIBUTE_TYPE_USER_TO_BACKEND_MAP.items()}


def map_attribute_type_user_to_backend(_type: str) -> str:
    return _ATTRIBUTE_TYPE_USER_TO_BACKEND_MAP.get(_type, _type)


def map_attribute_type_backend_to_user(_type: str) -> str:
    return _ATTRIBUTE_TYPE_BACKEND_TO_USER_MAP.get(_type, _type)
