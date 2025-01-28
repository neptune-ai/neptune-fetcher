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
    Literal,
    Optional,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.api_client import AuthenticatedClientBuilder
from neptune_fetcher.alpha.context import Context
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.attribute import fetch_attribute_definitions
from neptune_fetcher.alpha.internal.context import get_local_or_global_context
from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs


def fetch_experiments_table(
    experiments: Optional[Union[str, ExperimentFilter]] = None,
    attributes: Union[str, AttributeFilter] = "^sys/name$",
    sort_by: Union[str, Attribute] = Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    `experiments` - a filter specifying which experiments to include in the table
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object
    `sort_by` - an attribute name or an Attribute object specifying type and, optionally, aggregation
    `sort_direction` - 'asc' or 'desc'
    `limit` - maximum number of experiments to return; by default all experiments are returned.
    `type_suffix_in_column_names` - False by default. If True, columns of the returned DataFrame
        will be suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string", etc.
        If set to False, the method throws an exception if there are multiple types under one path.
    `context` - a Context object to be used; primarily useful for switching projects
    Returns a DataFrame similar to the Experiments Table in the UI, with an important difference:
    aggregates of metrics (min, max, avg, last, ...) are returned as sub-columns of a metric column. In other words,
    the returned DataFrame is indexed with a MultiIndex on (attribute name, attribute property).
    In case the user doesn't specify metrics' aggregates to be returned, only the `last` aggregate is returned.
    """
    context = get_local_or_global_context(ctx=context)
    client = AuthenticatedClientBuilder.build(context=context)
    project = identifiers.ProjectIdentifier(context.project)

    experiment_pages = fetch_experiment_sys_attrs(
        client=client,
        project_identifier=project,
        experiment_filter=experiments,
        sort_by=sort_by,
        sort_direction=sort_direction,
        limit=limit,
    )
    for experiment_sys in experiment_pages:
        attribute_names = fetch_attribute_definitions(
            client=client,
            project_identifiers=[project],
            experiment_identifiers=[
                identifiers.ExperimentIdentifier(project, info.sys_id) for info in experiment_sys.items
            ],
            attribute_filter=attributes,
        )
