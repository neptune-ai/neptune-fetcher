import os
import random
import re
import time
import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_fetcher.alpha.filter import AttributeFilter
from neptune_fetcher.alpha.internal.attribute import find_attribute_definitions

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
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert attribute_names == ["sys/name"]


def test_find_attributes_two_strings(client, run_init_kwargs, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_eq=["sys/name", "sys/owner"], type_in=["string"])
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert set(attribute_names) == {"sys/name", "sys/owner"}


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
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert attribute_names == [path]


def test_find_attributes_all_types(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    common_path = unique_path("test_attribute_filter/test_find_attributes_all_types")
    now = time.time()
    data = {
        f"{common_path}/int-value": int(now),
        f"{common_path}/float-value": now,
        f"{common_path}/str-value": f"hello-{now}",
        f"{common_path}/bool-value": True,
        f"{common_path}/datetime-value": datetime.now(timezone.utc).replace(microsecond=0),
    }
    run.log_configs(data)

    steps, values = random_series()
    for step, value in zip(steps, values):
        run.log_metrics(data={f"{common_path}/float-series-value": value}, step=step)

    run.add_tags({"string-set-item"})  # the only way to write string-set type. It's implicit path is sys/tags
    run.wait_for_processing()

    all_names = list(data.keys()) + [f"{common_path}/float-series-value", "sys/tags"]

    #  when
    attribute_filter = AttributeFilter(name_eq=all_names)
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert set(attribute_names) == set(all_names)


def test_find_attributes_no_type_in(client, run_init_kwargs, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name")
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert attribute_names == ["sys/name"]


def test_find_attributes_regex_matches_all(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_matches_all="sys/.*_time", type_in=["datetime"])
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert set(attribute_names) == {"sys/creation_time", "sys/modification_time", "sys/ping_time"}


def test_find_attributes_regex_matches_none(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(
        name_matches_all="sys/.*_time", name_matches_none="modification", type_in=["datetime"]
    )
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert set(attribute_names) == {"sys/creation_time", "sys/ping_time"}


def test_find_attributes_multiple_projects(client, run_init_kwargs, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    project_id_2 = f"{project_id}-does-not-exist"
    # TODO: would be nice to have an existing second project, but it's still a valid test case
    experiment_id = ro_run._container_id

    #  when
    attribute_filter = AttributeFilter(name_eq="sys/name", type_in=["string"])
    attribute_names = find_attribute_definitions(
        client, [project_id, project_id, project_id_2], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert attribute_names == ["sys/name"]


def test_find_attributes_filter_or(client, run_init_kwargs, run, ro_run):
    # given
    project_id = run_init_kwargs["project"]
    experiment_id = ro_run._container_id

    common_path = unique_path("test_attribute_filter/test_find_attributes_filter_or")
    now = time.time()
    data = {
        f"{common_path}/int_value_a": int(now),
        f"{common_path}/int_value_b": int(now),
        f"{common_path}/float_value_a": now,
        f"{common_path}/float_value_b": now,
    }
    run.log_configs(data)
    run.wait_for_processing()

    #  when
    attribute_filter_1 = AttributeFilter(name_matches_all=f"^{re.escape(common_path)}/.*_value_a$", type_in=["int"])
    attribute_filter_2 = AttributeFilter(name_matches_all=f"^{re.escape(common_path)}/.*_value_b$", type_in=["float"])
    attribute_filter = attribute_filter_1 | attribute_filter_2
    attribute_names = find_attribute_definitions(
        client, [project_id], [experiment_id], attribute_filter=attribute_filter
    )

    # then
    assert set(attribute_names) == {f"{common_path}/int_value_a", f"{common_path}/float_value_b"}
