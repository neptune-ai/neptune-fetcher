from unittest.mock import (
    ANY,
    call,
    patch,
)

import pytest

from neptune_fetcher import alpha as npt
from neptune_fetcher.alpha.filters import AttributeFilter
from neptune_fetcher.alpha.internal.identifiers import (
    ProjectIdentifier,
    RunIdentifier,
    SysId,
    SysName,
)
from neptune_fetcher.alpha.internal.retrieval import util
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.retrieval.search import ExperimentSysAttrs
from neptune_fetcher.alpha.internal.retrieval.series import RunAttributeDefinition


@pytest.mark.parametrize(
    "experiment_length, exp_number, attribute_length, attr_number, expected_calls",
    [
        (100, 0, 100, 0, []),
        (100, 1, 100, 0, []),
        (100, 1, 100, 1, [1]),
        (1000, 1, 100, 400, [400]),
        (1000, 1, 1000, 400, [220, 180]),
        (1000, 1, 10000, 400, [22] * 18 + [4]),
    ],
)
def test_fetch_string_series_values_composition_patched(
    experiment_length, exp_number, attribute_length, attr_number, expected_calls
):
    #  given
    project = ProjectIdentifier("project")
    npt.set_project(project)
    npt.set_api_token("irrelevant")
    experiments = [
        ExperimentSysAttrs(sys_id=SysId(f"{i:0{experiment_length}d}"), sys_name=SysName("irrelevant"))
        for i in range(exp_number)
    ]
    attributes = [AttributeDefinition(name=f"{i:0{attribute_length}d}", type="irrelevant") for i in range(attr_number)]
    run_attribute_definitions = [
        RunAttributeDefinition(
            run_identifier=RunIdentifier(project_identifier=project, sys_id=experiment.sys_id),
            attribute_definition=attribute,
        )
        for experiment in experiments
        for attribute in attributes
    ]

    # when
    with (
        patch("neptune_fetcher.alpha.internal.composition.fetch_series.get_client") as get_client,
        patch(
            "neptune_fetcher.alpha.internal.retrieval.search.fetch_experiment_sys_attrs"
        ) as fetch_experiment_sys_attrs,
        patch(
            "neptune_fetcher.alpha.internal.retrieval.attribute_definitions.fetch_attribute_definitions_single_filter"
        ) as fetch_attribute_definitions_single_filter,
        patch("neptune_fetcher.alpha.internal.retrieval.series.fetch_series_values") as fetch_series_values,
    ):
        get_client.return_value = None
        fetch_experiment_sys_attrs.return_value = iter([util.Page(experiments)])
        fetch_attribute_definitions_single_filter.return_value = iter([util.Page(attributes)])
        fetch_series_values.return_value = iter([])

        npt.fetch_series(experiments="ignored", attributes=AttributeFilter(name_eq="ignored"))

    # then
    call_sizes = [
        len(fetch_series_values.call_args_list[i].kwargs["run_attribute_definitions"])
        for i in range(fetch_series_values.call_count)
    ]
    assert call_sizes == expected_calls
    fetch_series_values.assert_has_calls(
        [
            call(
                run_attribute_definitions=run_attribute_definitions[start:end],
                client=ANY,
                include_inherited=ANY,
                step_range=ANY,
                tail_limit=ANY,
            )
            for start, end in _edges(expected_calls)
        ]
    )


def _edges(sizes):
    start = 0
    for size in sizes:
        end = start + size
        yield start, end
        start = end
