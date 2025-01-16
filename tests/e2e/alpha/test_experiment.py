import os

from neptune_fetcher.alpha.internal.experiment import find_experiments

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")



def test_find_experiments_by_name(client, run_init_kwargs):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = run_init_kwargs["experiment_name"]

    #  when
    experiment_filter = f'`sys/name`:string = "{experiment_name}"'
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == [experiment_name]


def test_find_experiments_by_name_not_found(client, run_init_kwargs):
    # given
    project_id = run_init_kwargs["project"]
    experiment_name = "test_find_experiments_by_name_not_found"

    #  when
    experiment_filter = f'`sys/name`:string = "{experiment_name}"'
    experiment_names = find_experiments(client, project_id, experiment_filter)

    # then
    assert experiment_names == []
