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
    Generator,
    Literal,
    Optional,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha import Context
from neptune_fetcher.alpha import context as _context
from neptune_fetcher.alpha.filter import (
    Attribute,
    AttributeFilter,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal import api_client as _api_client
from neptune_fetcher.alpha.internal import attribute as _attribute
from neptune_fetcher.alpha.internal import experiment as _experiment
from neptune_fetcher.alpha.internal import identifiers as _identifiers
from neptune_fetcher.alpha.internal import output as _output
from neptune_fetcher.alpha.internal import types as _types
from neptune_fetcher.alpha.internal import util as _util

__all__ = ("fetch_experiments_table",)


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
    valid_context = _context.validate_context(context or _context.get_context())
    client = _api_client.get_client(valid_context)
    project = _identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    if isinstance(experiments, str):
        experiments_filter: Optional[ExperimentFilter] = ExperimentFilter.matches_all(
            Attribute("sys/name", type="string"), experiments
        )
    else:
        experiments_filter = experiments

    if isinstance(attributes, str):
        attributes_filter = AttributeFilter(name_matches_all=attributes)
    else:
        attributes_filter = attributes

    if isinstance(sort_by, str):
        sort_by_attribute = Attribute(sort_by, type="string")  # TODO: infer type?
    else:
        sort_by_attribute = sort_by

    experiment_name_mapping: dict[_identifiers.SysId, _identifiers.SysName] = {}
    result_by_id: dict[_identifiers.SysId, list[_types.AttributeValue]] = {}
    with _util.create_executor() as executor:
        experiment_pages = _experiment.fetch_experiment_sys_attrs(
            client=client,
            project_identifier=project,
            experiment_filter=experiments_filter,
            sort_by=sort_by_attribute,
            sort_direction=sort_direction,
            limit=limit,
            executor=executor,
        )

        def process_experiment_page_stateful(
            page: _util.Page[_experiment.ExperimentSysAttrs],
        ) -> list[_identifiers.ExperimentIdentifier]:
            for experiment in page.items:
                result_by_id[experiment.sys_id] = []  # I assume that dict preserves the order set here
                experiment_name_mapping[experiment.sys_id] = experiment.sys_name  # TODO: check for duplicate names?
            return [_identifiers.ExperimentIdentifier(project, experiment.sys_id) for experiment in page.items]

        def go_fetch_attribute_definitions(
            experiment_identifiers: list[_identifiers.ExperimentIdentifier],
        ) -> Generator[_util.Page[_attribute.AttributeDefinition], None, None]:
            return _attribute.fetch_attribute_definitions(
                client=client,
                project_identifiers=[project],
                experiment_identifiers=experiment_identifiers,
                attribute_filter=attributes_filter,
                executor=executor,
            )

        def go_fetch_attribute_values(
            experiment_identifiers: list[_identifiers.ExperimentIdentifier],
            attribute_definition_page: _util.Page[_attribute.AttributeDefinition],
        ) -> Generator[_util.Page[_types.AttributeValue], None, None]:
            return _attribute.fetch_attribute_values(
                client=client,
                project_identifier=project,
                experiment_identifiers=experiment_identifiers,
                attribute_definitions=attribute_definition_page.items,
                executor=executor,
            )

        output = _util.process_concurrently(
            items=(process_experiment_page_stateful(page) for page in experiment_pages),
            executor=executor,
            downstream=lambda experiment_identifiers: _util.process_concurrently(
                items=go_fetch_attribute_definitions(experiment_identifiers),
                executor=executor,
                downstream=lambda definitions_page: _util.process_concurrently(
                    items=go_fetch_attribute_values(experiment_identifiers, definitions_page),
                    executor=executor,
                    downstream=_util.return_value,
                ),
            ),
        )
        attribute_values_pages: list[_util.Page[_types.AttributeValue]] = _util.gather_results(output)

        for attribute_values_page in attribute_values_pages:
            for attribute_value in attribute_values_page.items:
                sys_id = attribute_value.experiment_identifier.sys_id
                result_by_id[sys_id].append(attribute_value)

    result_by_name = _map_keys_preserving_order(result_by_id, experiment_name_mapping)
    dataframe = _output.convert_experiment_table_to_dataframe(
        result_by_name, type_suffix_in_column_names=type_suffix_in_column_names
    )
    return dataframe


def _map_keys_preserving_order(
    result_by_id: dict[_identifiers.SysId, list[_types.AttributeValue]],
    experiment_name_mapping: dict[_identifiers.SysId, _identifiers.SysName],
) -> dict[_identifiers.SysName, list[_types.AttributeValue]]:
    result_by_name = {}
    for sys_id, values in result_by_id.items():
        sys_name = experiment_name_mapping[sys_id]
        result_by_name[sys_name] = values
    return result_by_name
