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

from __future__ import annotations

from typing import Generator

from neptune_fetcher.alpha.internal import (
    env,
    identifiers,
)
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as adef

_UUID_SIZE = 50


def _attribute_definition_size(attr: adef.AttributeDefinition) -> int:
    return len(attr.name.encode("utf-8"))


def split_sys_ids(
    sys_ids: list[identifiers.SysId],
) -> Generator[list[identifiers.SysId]]:
    """
    Splits a sequence of sys ids into batches of size at most `NEPTUNE_FETCHER_QUERY_SIZE_LIMIT`.
    Use before fetching attribute definitions.
    """
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get()
    identifier_num_limit = max(query_size_limit // _UUID_SIZE, 1)

    identifier_num = len(sys_ids)
    batch_num = _ceil_div(identifier_num, identifier_num_limit)

    if batch_num <= 1:
        yield sys_ids
    else:
        batch_size = _ceil_div(identifier_num, batch_num)
        for i in range(0, identifier_num, batch_size):
            yield sys_ids[i : i + batch_size]


def split_sys_ids_attributes(
    sys_ids: list[identifiers.SysId],
    attribute_definitions: list[adef.AttributeDefinition],
) -> Generator[tuple[list[identifiers.SysId], list[adef.AttributeDefinition]]]:
    """
    Splits a pair of sys ids and attribute_definitions into batches that:
    When their length is added it is of size at most `NEPTUNE_FETCHER_QUERY_SIZE_LIMIT`.
    When their item count is multiplied, it is at most `NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE`.
    Use before fetching attribute values.
    """
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get()
    attribute_values_batch_size = env.NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.get()

    if not attribute_definitions:
        return

    attribute_batches = _split_attribute_definitions(attribute_definitions)
    max_attribute_batch_size = max(
        sum(_attribute_definition_size(attr) for attr in batch) for batch in attribute_batches
    )
    max_attribute_batch_len = max(len(batch) for batch in attribute_batches)

    sys_id_batch: list[identifiers.SysId] = []
    total_batch_size = max_attribute_batch_size
    for experiment in sys_ids:
        if sys_id_batch and (
            (len(sys_id_batch) + 1) * max_attribute_batch_len > attribute_values_batch_size
            or total_batch_size + _UUID_SIZE > query_size_limit
        ):
            for attribute_batch in attribute_batches:
                yield sys_id_batch, attribute_batch
            sys_id_batch = []
            total_batch_size = max_attribute_batch_size
        sys_id_batch.append(experiment)
        total_batch_size += _UUID_SIZE
    if sys_id_batch:
        for attribute_batch in attribute_batches:
            yield sys_id_batch, attribute_batch


def _split_attribute_definitions(
    attribute_definitions: list[adef.AttributeDefinition],
) -> list[list[adef.AttributeDefinition]]:
    query_size_limit = env.NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.get() - _UUID_SIZE
    attribute_values_batch_size = env.NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.get()

    attribute_batches = []
    current_batch: list[adef.AttributeDefinition] = []
    current_batch_size = 0
    for attr in attribute_definitions:
        attr_size = _attribute_definition_size(attr)
        if current_batch and (
            len(current_batch) >= attribute_values_batch_size or current_batch_size + attr_size > query_size_limit
        ):
            attribute_batches.append(current_batch)
            current_batch = []
            current_batch_size = 0
        current_batch.append(attr)
        current_batch_size += attr_size

    if current_batch:
        attribute_batches.append(current_batch)

    return attribute_batches


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b
