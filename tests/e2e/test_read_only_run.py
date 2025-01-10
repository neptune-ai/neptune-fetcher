import os

from neptune_fetcher import ReadOnlyRun

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_FIXED_PROJECT")


def test__eager_loading_structure(project, all_experiment_ids):
    # Test default behavior (eager loading enabled)
    run = ReadOnlyRun(project, custom_id=all_experiment_ids[0])
    field_names = list(run.field_names)
    assert len(field_names) > 0, "Field names should be available immediately with eager loading"

    # Test with eager loading disabled
    run_no_eager = ReadOnlyRun(project, custom_id=all_experiment_ids[0], eager_load_attribute_definitions=False)
    field_names_lazy = list(run_no_eager.field_names)
    assert len(field_names_lazy) > 0, "Field names should be available after lazy loading"
    assert field_names == field_names_lazy, "Field names should be the same regardless of eager loading setting"

    # Test that accessing a field works in both cases
    assert run["sys/custom_run_id"].fetch() == all_experiment_ids[0]
    assert run_no_eager["sys/custom_run_id"].fetch() == all_experiment_ids[0]


def test__lazy_loading_structure_to_access_fields(project, all_experiment_ids):
    run = ReadOnlyRun(project, custom_id=all_experiment_ids[0], eager_load_attribute_definitions=False)

    assert run["sys/custom_run_id"].fetch() == all_experiment_ids[0]
    assert len(run._structure) == 1
    assert set(run._structure.keys()) == {"sys/custom_run_id"}
