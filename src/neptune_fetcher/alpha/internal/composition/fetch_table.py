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
from collections import defaultdict
from typing import (
    Generator,
    Literal,
    Optional,
    Union,
)

import pandas as pd

from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import context as _context
from neptune_fetcher.alpha.internal import (
    identifiers,
    output_format,
)
from neptune_fetcher.alpha.internal.composition import attributes as _attributes
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.retrieval import attribute_definitions as att_defs
from neptune_fetcher.alpha.internal.retrieval import attribute_values as att_vals
from neptune_fetcher.alpha.internal.retrieval import (
    search,
    split,
    util,
)

__all__ = (
    "fetch_experiments_table",
    "fetch_runs_table",
)


def fetch_experiments_table(
    experiments: Optional[Union[str, Filter]] = None,
    attributes: Union[str, AttributeFilter] = "^sys/name$",
    sort_by: Union[str, Attribute] = Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[_context.Context] = None,
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
    _validate_limit(limit)
    _sort_direction = _validate_sort_direction(sort_direction)
    valid_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    if isinstance(experiments, str):
        experiments_filter: Optional[Filter] = Filter.matches_all(Attribute("sys/name", type="string"), experiments)
    else:
        experiments_filter = experiments

    if isinstance(attributes, str):
        attributes_filter = AttributeFilter(name_matches_all=attributes)
    else:
        attributes_filter = attributes

    if isinstance(sort_by, str):
        sort_by_attribute = Attribute(sort_by)
    else:
        sort_by_attribute = sort_by

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):

        type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project,
            experiment_filter=experiments_filter,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        type_inference.infer_attribute_types_in_sort_by(
            client=client,
            project_identifier=project,
            experiment_filter=experiments_filter,
            sort_by=sort_by_attribute,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )

        experiment_name_mapping: dict[identifiers.SysId, identifiers.SysName] = {}
        result_by_id: dict[identifiers.SysId, list[att_vals.AttributeValue]] = {}
        selected_aggregations: dict[att_defs.AttributeDefinition, set[str]] = defaultdict(set)

        def go_fetch_experiment_sys_attrs() -> Generator[util.Page[search.ExperimentSysAttrs], None, None]:
            return search.fetch_experiment_sys_attrs(
                client=client,
                project_identifier=project,
                _filter=experiments_filter,
                sort_by=sort_by_attribute,
                sort_direction=_sort_direction,
                limit=limit,
            )

        def process_experiment_page_stateful(
            page: util.Page[search.ExperimentSysAttrs],
        ) -> list[identifiers.RunIdentifier]:
            for experiment in page.items:
                result_by_id[experiment.sys_id] = []  # I assume that dict preserves the order set here
                experiment_name_mapping[experiment.sys_id] = experiment.sys_name  # TODO: check for duplicate names?
            return [identifiers.RunIdentifier(project, experiment.sys_id) for experiment in page.items]

        def go_fetch_attribute_definitions(
            experiment_identifiers: list[identifiers.RunIdentifier],
        ) -> Generator[util.Page[_attributes.AttributeDefinitionAggregation], None, None]:
            return _attributes.fetch_attribute_definition_aggregations(
                client=client,
                project_identifiers=[project],
                experiment_identifiers=experiment_identifiers,
                attribute_filter=attributes_filter,
                executor=fetch_attribute_definitions_executor,
            )

        def go_fetch_attribute_values(
            experiment_identifiers: list[identifiers.RunIdentifier],
            attribute_definitions: list[att_defs.AttributeDefinition],
        ) -> Generator[util.Page[att_vals.AttributeValue], None, None]:
            return att_vals.fetch_attribute_values(
                client=client,
                project_identifier=project,
                run_identifiers=experiment_identifiers,
                attribute_definitions=attribute_definitions,
            )

        def filter_definitions(
            attribute_definition_aggregation_page: util.Page[_attributes.AttributeDefinitionAggregation],
        ) -> list[att_defs.AttributeDefinition]:
            return [
                item.attribute_definition
                for item in attribute_definition_aggregation_page.items
                if item.aggregation is None
            ]

        def collect_aggregations(
            attribute_definition_aggregation_page: util.Page[_attributes.AttributeDefinitionAggregation],
        ) -> dict[att_defs.AttributeDefinition, set[str]]:
            aggregations: dict[att_defs.AttributeDefinition, set[str]] = defaultdict(set)
            for item in attribute_definition_aggregation_page.items:
                if item.aggregation is not None:
                    aggregations[item.attribute_definition].add(item.aggregation)
            return aggregations

        output = concurrency.generate_concurrently(
            items=(process_experiment_page_stateful(page) for page in go_fetch_experiment_sys_attrs()),
            executor=executor,
            downstream=lambda experiment_identifiers: concurrency.generate_concurrently(
                items=split.split_runs(experiment_identifiers),
                executor=executor,
                downstream=lambda experiment_identifiers_split: concurrency.generate_concurrently(
                    items=go_fetch_attribute_definitions(experiment_identifiers_split),
                    executor=executor,
                    downstream=lambda definition_aggs_page: concurrency.fork_concurrently(
                        item=definition_aggs_page,
                        executor=executor,
                        downstreams=[
                            lambda _definition_aggs_page: concurrency.generate_concurrently(
                                items=split.split_runs_attributes(
                                    experiment_identifiers_split, filter_definitions(_definition_aggs_page)
                                ),
                                executor=executor,
                                downstream=lambda split_pair: concurrency.generate_concurrently(
                                    items=go_fetch_attribute_values(split_pair[0], split_pair[1]),
                                    executor=executor,
                                    downstream=concurrency.return_value,
                                ),
                            ),
                            lambda _definition_aggs_page: concurrency.return_value(
                                collect_aggregations(_definition_aggs_page)
                            ),
                        ],
                    ),
                ),
            ),
        )
        results: Generator[
            Union[util.Page[att_vals.AttributeValue], dict[att_defs.AttributeDefinition, set[str]]], None, None
        ] = concurrency.gather_results(output)

        for result in results:
            if isinstance(result, util.Page):
                attribute_values_page = result
                for attribute_value in attribute_values_page.items:
                    sys_id = attribute_value.run_identifier.sys_id
                    result_by_id[sys_id].append(attribute_value)
            elif isinstance(result, dict):
                aggregations = result
                for attribute_definition, aggregation_set in aggregations.items():
                    selected_aggregations[attribute_definition].update(aggregation_set)
            else:
                raise RuntimeError(f"Unexpected result type: {type(result)}")

    result_by_name = _map_keys_preserving_order(result_by_id, experiment_name_mapping)
    dataframe = output_format.convert_experiment_table_to_dataframe(
        result_by_name,
        selected_aggregations=selected_aggregations,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )
    return dataframe


def fetch_runs_table(
    runs: Optional[Union[str, Filter]] = None,
    attributes: Union[str, AttributeFilter] = "^sys/name$",
    sort_by: Union[str, Attribute] = Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
    context: Optional[_context.Context] = None,
) -> pd.DataFrame:
    ...


def _map_keys_preserving_order(
    result_by_id: dict[identifiers.SysId, list[att_vals.AttributeValue]],
    experiment_name_mapping: dict[identifiers.SysId, identifiers.SysName],
) -> dict[identifiers.SysName, list[att_vals.AttributeValue]]:
    result_by_name = {}
    for sys_id, values in result_by_id.items():
        sys_name = experiment_name_mapping[sys_id]
        result_by_name[sys_name] = values
    return result_by_name


def _validate_limit(limit: Optional[int]) -> None:
    """Validate that limit is either None or a positive integer."""
    if limit is not None:
        if not isinstance(limit, int):
            raise ValueError("limit must be None or an integer")
        if limit <= 0:
            raise ValueError("limit must be greater than 0")


def _validate_sort_direction(sort_direction: Literal["asc", "desc"]) -> Literal["asc", "desc"]:
    """Validate that sort_direction is either 'asc' or 'desc'."""
    if sort_direction not in ("asc", "desc"):
        raise ValueError("sort_direction must be either 'asc' or 'desc'")
    return sort_direction
