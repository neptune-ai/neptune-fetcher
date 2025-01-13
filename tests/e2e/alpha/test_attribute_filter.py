import os
import random
import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_fetcher.alpha.attribute_filter import (
    AttributeFilter,
    find_attributes,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def unique_path(prefix):
    return f"{prefix}__{datetime.now(timezone.utc).isoformat('-', 'seconds')}__{str(uuid.uuid4())[-4:]}"


def random_series(length=10, start_step=0):
    """Return a 2-tuple of step and value lists, both of length `length`"""
    assert length > 0
    assert start_step >= 0

    j = random.random()
    # Round to 0 to avoid floating point errors
    steps = [round((j + x) ** 2.0, 0) for x in range(start_step, length)]
    values = [round((j + x) ** 3.0, 0) for x in range(len(steps))]

    return steps, values


def test_find_attributes_single_string(client, run_init_kwargs, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attribute_names = find_attributes(client, project_id, [experiment_id], attribute_filter=attribute_filter)

    # then
    assert attribute_names == ["sys/name"]


def test_find_attributes_single_series(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    path = unique_path("test_attribute_filter/test_find_attributes_single_series")
    steps, values = random_series()
    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)
    run.wait_for_processing()

    #  when
    attribute_filter = AttributeFilter(name_eq=path, type_in=["float_series"])
    attribute_names = find_attributes(client, project_id, [experiment_id], attribute_filter=attribute_filter)

    # then
    assert attribute_names == [path]


def test_find_attributes_regex_matches_all(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attribute_names = find_attributes(client, project_id, [experiment_id], attribute_filter=attribute_filter)

    # then
    assert attribute_names == ["sys/creation_time", "sys/modification_time", "sys/ping_time"]


def test_find_attributes_regex_matches_none(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_matches_none="^sys/.*", type_in=["datetime"])
    attribute_names = find_attributes(client, project_id, [experiment_id], attribute_filter=attribute_filter)

    # then
    assert attribute_names == ["test_start_time"]


def test_find_attributes_regex_matches_all_none(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(
        name_matches_all="sys/.*_time", name_matches_none="modification", type_in=["datetime"]
    )
    attribute_names = find_attributes(client, project_id, [experiment_id], attribute_filter=attribute_filter)

    # then
    assert attribute_names == ["sys/creation_time", "sys/ping_time"]
