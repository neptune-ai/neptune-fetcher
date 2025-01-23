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

from typing import (
    List,
    Optional,
    Union,
)

from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha.filter import (
    AttributeFilter,
    ExperimentFilter,
)


def list_attributes(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    attributes: Optional[Union[str, AttributeFilter]] = None,
    context: Optional[Context] = None,
) -> List[str]:
    """
     List attributes' names in project.
     Optionally filter by experiments and attributes.
     `experiments` - a filter specifying experiments to which the attributes belong
         - a regex that experiment name must match, or
         - a Filter object
     `attributes` - a filter specifying which attributes to include in the table
         - a regex that attribute name must match, or
         - an AttributeFilter object;
           If `AttributeFilter.aggregations` is set, an exception will be raised
           as they're not supported in this function.
    `context` - a Context object to be used; primarily useful for switching projects

     Returns a list of unique attribute names in experiments matching the filter.
    """
    ...
