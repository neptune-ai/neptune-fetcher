import pytest

from neptune_fetcher.alpha import set_project

from neptune_scale import Run
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.retrieval.search import fetch_experiment_sys_attrs
from neptune_fetcher.alpha.filters import Filter
from tests.e2e.alpha.data import (
    NOW,
    PATH,
    TEST_DATA,
)
from datetime import timedelta

import time


@pytest.fixture(autouse=True)
def context(project):
    set_project(project.project_identifier)


@pytest.fixture(scope="module", autouse=True)
def run_with_attributes(project, client):
    runs = {}
    for experiment in TEST_DATA.experiments:
        project_id = project.project_identifier

        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project_id),
                Filter.name_in(experiment.name),
            )
        )
        if existing.items:
            continue

        run = Run(
            project=project_id,
            run_id=experiment.run_id,
            experiment_name=experiment.name,
        )

        run.log_configs(experiment.config)
        # This is how neptune-scale allows setting string set values currently
        run.log(tags_add=experiment.string_sets)

        for step in range(len(experiment.float_series[f"{PATH}/metrics/step"])):
            metrics_data = {path: values[step] for path, values in experiment.float_series.items()}
            metrics_data[f"{PATH}/metrics/step"] = step
            run.log_metrics(data=metrics_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

            series_data = {path: values[step] for path, values in experiment.string_series.items()}
            run.log_string_series(data=series_data, step=step, timestamp=NOW + timedelta(seconds=int(step)))

        run.assign_files(experiment.files)

        runs[experiment.name] = run
    for run in runs.values():
        run.close()

    # Make sure all experiments are visible in the system before starting tests
    for _ in range(15):
        existing = next(
            fetch_experiment_sys_attrs(
                client,
                identifiers.ProjectIdentifier(project.project_identifier),
                Filter.name_in(*TEST_DATA.experiment_names),
            )
        )

        if len(existing.items) == len(TEST_DATA.experiment_names):
            return runs

        time.sleep(1)

    raise RuntimeError("Experiments did not appear in the system in time")
