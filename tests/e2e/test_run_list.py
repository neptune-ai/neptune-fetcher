import os

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


def test__list_runs(project, all_run_ids, all_experiment_ids, sys_columns_set):
    result = list(project.list_runs())
    ids = all_run_ids + all_experiment_ids
    print(result)
    assert all(set(run.keys()) == sys_columns_set for run in result), "All runs must have sys columns"

    assert len(result) == len(ids)
    assert set(run["sys/custom_run_id"] for run in result) == set(ids)


def test__list_experiments(project, all_experiment_ids, sys_columns_set):
    result = list(project.list_experiments())

    assert all(set(run.keys()) == sys_columns_set for run in result), "All experiments must have sys columns"

    assert len(result) == len(all_experiment_ids)
    assert set(run["sys/custom_run_id"] for run in result) == set(all_experiment_ids)
    assert all(len(run["sys/name"]) for run in result), "All experiments must have non-empty names"


def test__fetch_runs(project, all_run_ids, all_experiment_ids, sys_columns):
    result = project.fetch_runs()

    ids = all_run_ids + all_experiment_ids

    assert all(col in result.columns for col in sys_columns), "All sys columns must be present"
    assert len(result) == len(ids)
    assert set(result["sys/custom_run_id"]) == set(ids)


def test__fetch_experiments(project, all_experiment_ids, sys_columns):
    result = project.fetch_experiments()

    assert all(col in result.columns for col in sys_columns), "All sys columns must be present"
    assert len(result) == len(all_experiment_ids)
    assert set(result["sys/custom_run_id"]) == set(all_experiment_ids)
    assert all(len(name) for name in result["sys/name"]), "All experiments must have non-empty names"
