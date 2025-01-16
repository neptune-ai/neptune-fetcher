import os
import random

import pytest

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


@pytest.mark.parametrize("eager_load_fields", [True, False])
def test_fetch_read_only_experiments(project, all_experiment_ids, eager_load_fields):
    experiments = list(project.fetch_read_only_experiments(eager_load_fields=eager_load_fields))

    assert len(experiments) == len(all_experiment_ids)
    for exp in experiments:
        assert exp["sys/custom_run_id"].fetch() in all_experiment_ids


def test_fetch_read_only_with_experiment_names(project, all_experiment_ids):
    exp_names = ["exp2", "exp3"]
    experiments = list(project.fetch_read_only_experiments(names=exp_names))

    assert len(experiments) == 2
    assert set(exp_names) == {exp["sys/name"].fetch() for exp in experiments}


@pytest.mark.parametrize("eager_load_fields", [True, False])
def test_fetch_read_only_runs(project, all_experiment_ids, eager_load_fields):
    runs = list(project.fetch_read_only_runs(eager_load_fields=eager_load_fields))

    assert len(runs) == len(project.fetch_runs())

    runs = random.sample(runs, 3)

    custom_ids = [run["sys/custom_run_id"].fetch() for run in runs]
    sys_ids = [run["sys/id"].fetch() for run in runs]

    filtered = list(
        project.fetch_read_only_runs(with_ids=sys_ids, custom_ids=custom_ids, eager_load_fields=eager_load_fields)
    )

    assert len(filtered) == len(runs)
    assert set(sys_ids) == {run["sys/id"].fetch() for run in runs}
