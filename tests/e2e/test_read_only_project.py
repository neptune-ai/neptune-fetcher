import os
import random

import pytest

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


@pytest.mark.parametrize("eager_load_fields", [True, False])
def test_fetch_read_only_experiments(project, all_experiment_ids, eager_load_fields):
    experiments = list(project.fetch_read_only_experiments(names=None, eager_load_fields=eager_load_fields))
    experiments_empty_names = list(project.fetch_read_only_experiments(names=[], eager_load_fields=eager_load_fields))
    assert len(experiments) == len(experiments_empty_names) == 0


def test_fetch_read_only_with_experiment_names(project, all_experiment_names):
    exp_names = random.sample(all_experiment_names, 3)
    experiments = list(project.fetch_read_only_experiments(names=exp_names))

    assert len(experiments) == len(exp_names)
    assert set(exp_names) == {exp["sys/name"].fetch() for exp in experiments}


@pytest.mark.parametrize("eager_load_fields", [True, False])
def test_fetch_read_only_runs(project, all_experiment_ids, eager_load_fields):
    assert [] == list(project.fetch_read_only_runs(eager_load_fields=eager_load_fields))
    assert [] == list(project.fetch_read_only_runs(with_ids=[], custom_ids=[], eager_load_fields=eager_load_fields))

    runs_ids = random.sample(all_experiment_ids, 3)
    filtered = list(project.fetch_read_only_runs(custom_ids=runs_ids, eager_load_fields=eager_load_fields))

    assert len(filtered) == len(runs_ids)
    assert set(runs_ids) == {run["sys/custom_run_id"].fetch() for run in filtered}

    sys_ids = [run["sys/id"].fetch() for run in filtered]

    filtered_by_sys_id = list(project.fetch_read_only_runs(with_ids=sys_ids, eager_load_fields=eager_load_fields))
    assert set(runs_ids) == {run["sys/custom_run_id"].fetch() for run in filtered_by_sys_id}

    duplicated = list(
        project.fetch_read_only_runs(custom_ids=runs_ids, with_ids=sys_ids, eager_load_fields=eager_load_fields)
    )
    # custom_ids=runs_ids, with_ids=sys_ids duplicates the results
    assert len(duplicated) == (len(runs_ids) * 2)
    assert set(runs_ids) == {run["sys/custom_run_id"].fetch() for run in duplicated}
