import os
import random
import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_fetcher.alpha.filter import (
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


def test_find_attributes_single_series(run_init_kwargs, run, client):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = run_init_kwargs["experiment_name"]
    path = unique_path("test_filter/test_find_attributes_single_series")
    steps, values = random_series()

    for step, value in zip(steps, values):
        run.log_metrics(data={path: value}, step=step)
    run.wait_for_processing()

    #  when
    attribute_names = find_attributes(client, project_id, [experiment_name], AttributeFilter(name_matches_all=".*"))

    assert attribute_names == [path, "hmm"]
