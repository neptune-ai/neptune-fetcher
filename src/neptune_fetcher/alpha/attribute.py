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
#

__all__ = ("list_attributes",)

from typing import (
    Optional,
    Union,
)

from neptune_fetcher.alpha.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import attribute
from neptune_fetcher.alpha.internal.api_client import get_client
from neptune_fetcher.alpha.internal.identifiers import ProjectIdentifier


def list_attributes(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    attributes: Optional[Union[str, AttributeFilter]] = None,
    context: Optional[Context] = None,
) -> list[str]:
    """
    List attributes' names in project.
    Optionally filter by experiments and attributes.
    `experiments` - a filter specifying experiments to which the attributes belong
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object;
            If `AttributeFilter.aggregations` is set, an exception will be raised as they're
            not supported in this function.

    Returns a list of unique attribute names in experiments matching the filter.
    """

    valid_context = validate_context(context or get_context())
    client = get_client(valid_context)

    assert valid_context.project is not None  # mypy TODO: remove at some point
    project_id = ProjectIdentifier(valid_context.project)

    if isinstance(experiments, str):
        experiments = ExperimentFilter.matches_all(Attribute("sys/name", type="string"), regex=experiments)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=[attributes])

    result = attribute.list_attributes(client, project_id, experiment_filter=experiments, attribute_filter=attributes)

    return sorted(set(result))
