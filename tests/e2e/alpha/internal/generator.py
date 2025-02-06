from collections import defaultdict
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from optparse import Option
from typing import Optional, Union
import uuid
import itertools
from neptune_scale import Run
from neptune_api import AuthenticatedClient

from neptune_fetcher.alpha import Context, get_context
from neptune_fetcher.alpha.internal.api_client import get_client
import pytest

METRICS_COUNT = 10
POINTS_PER_METRIC = 1_000
FORK_POINTS = [400, 800]
RUN_COUNT = 5

AttributeName = str
Step = float
Value = float


@dataclass
class GeneratedRun:
    custom_run_id: str
    experiment_name: str
    fork_run_id: Union[str, None]
    fork_level: Optional[int]
    fork_point: Optional[int]
    configs: dict[AttributeName, Union[float, bool, int, str, datetime, list, set, tuple]]
    metrics: dict[AttributeName, dict[Step, Value]]


# Tree structure:
#
# linear_history_tree:
#
# root (level: None)
#   └── fork1 (level: 1, fork_point: 400)
#         └── fork2 (level: 2, fork_point: 800)
#

linear_history_exp_name = "exp_with_linear_history"
linear_history_tree = [
    GeneratedRun(
        custom_run_id="linear_history_root",
        experiment_name=linear_history_exp_name,
        fork_level=None,
        fork_point=None,
        fork_run_id=None,
        configs={
            "int-value": 1,
            "float-value": 1.0,
            "str-value": "hello_1",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {0: 0.1, 100: 0.2, 200: 0.3, 300: 0.4, 400: 0.5},
            "foo1": {0: 1.1, 100: 1.2, 200: 1.3, 300: 1.4, 400: 1.5},
            "unique1/0": {0: 2.1, 100: 2.2, 200: 2.3, 300: 2.4, 400: 2.5},
        },
    ),
    GeneratedRun(
        custom_run_id="linear_history_fork1",
        fork_run_id="linear_history_root",
        experiment_name=linear_history_exp_name,
        fork_level=1,
        fork_point=400,
        configs={
            "int-value": 2,
            "float-value": 2.0,
            "str-value": "hello_2",
            "bool-value": True,
            "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {400: 0.5, 500: 0.6, 600: 0.7, 700: 0.8},
            "foo1": {400: 1.5, 500: 1.6, 600: 1.7, 700: 1.8},
            "unique2/0": {400: 3.1, 500: 3.2, 600: 3.3, 700: 3.4},
        },
    ),
    GeneratedRun(
        custom_run_id="linear_history_fork2",
        fork_run_id="linear_history_fork1",
        experiment_name=linear_history_exp_name,
        fork_level=2,
        fork_point=800,
        configs={
            "int-value": 3,
            "float-value": 3.0,
            "str-value": "hello_3",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {800: 0.8, 900: 0.9, 1000: 1.0},
            "foo1": {800: 1.8, 900: 1.9, 1000: 2.0},
            "unique3/0": {800: 4.1, 900: 4.2, 1000: 4.3},
        },
    ),
]

# Tree structure:
#
# forked_history_tree:
#
# root (level: None)
#   ├── fork1 (level: 1, fork_point: 400)
#   └── fork2 (level: 1, fork_point: 800)

forked_history_experiment_name = "epx_with_forked_history"

forked_history_tree = [
    GeneratedRun(
        custom_run_id="forked_history_root",
        experiment_name=forked_history_experiment_name,
        fork_level=None,
        fork_point=None,
        fork_run_id=None,
        configs={
            "int-value": 1,
            "float-value": 1.0,
            "str-value": "hello_1",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {0: 0.1, 100: 0.2, 200: 0.3, 300: 0.4, 400: 0.5},
            "foo1": {0: 1.1, 100: 1.2, 200: 1.3, 300: 1.4, 400: 1.5},
            "unique1/0": {0: 2.1, 100: 2.2, 200: 2.3, 300: 2.4, 400: 2.5},
        },
    ),
    GeneratedRun(
        custom_run_id="forked_history_fork1",
        experiment_name=forked_history_experiment_name,
        fork_level=1,
        fork_point=400,
        fork_run_id="forked_history_root",  # References root
        configs={
            "int-value": 2,
            "float-value": 2.0,
            "str-value": "hello_2",
            "bool-value": True,
            "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {400: 0.5, 500: 0.6, 600: 0.7, 700: 0.8, 800: 0.9},
            "foo1": {400: 1.5, 500: 1.6, 600: 1.7, 700: 1.8, 800: 1.9},
            "unique2/0": {400: 3.1, 500: 3.2, 600: 3.3, 700: 3.4, 800: 3.5},
        },
    ),
    GeneratedRun(
        custom_run_id="forked_history_fork2",
        experiment_name=forked_history_experiment_name,
        fork_level=1,
        fork_point=800,
        fork_run_id="forked_history_root",  # References root
        configs={
            "int-value": 3,
            "float-value": 3.0,
            "str-value": "hello_3",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {800: 0.9, 900: 1.0, 1000: 1.1},
            "foo1": {800: 1.9, 900: 2.0, 1000: 2.1},
            "unique3/0": {800: 4.1, 900: 4.2, 1000: 4.3},
        },
    ),
]


def create_project(client: AuthenticatedClient, workspace: str, name: str):
    body = {"organizationIdentifier": workspace, "name": name, "visibility": "priv"}
    args = {
        "method": "post",
        "url": "/api/backend/v1/projects",
        "json": body,
    }

    response = client.get_httpx_client().request(**args)
    response.raise_for_status()


@pytest.fixture(scope="session")
def test_log_runs(client: AuthenticatedClient):
    workspace = "neptune-e2e"  # TODO
    project_name = f"pye2e-alpha-runs-{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"

    project_id = f"{workspace}/{project_name}"

    create_project(client, workspace, project_name)

    log_runs(project_id, list(itertools.chain(linear_history_tree, forked_history_tree)))
    return project_id


def log_run(generated: GeneratedRun, e2e_alpha_project: str):
    with Run(
        project=e2e_alpha_project,
        run_id=generated.custom_run_id,
        experiment_name=generated.experiment_name,
        fork_run_id=generated.fork_run_id,
        fork_step=generated.fork_point,
    ) as run:
        run.log_configs(data=generated.configs)
        for metric_name, metric_values in generated.metrics.items():
            for step, value in metric_values.items():
                run.log_metrics(step=step, data={metric_name: value})


def log_runs(e2e_alpha_project: str, runs: list[GeneratedRun]):
    max_level = max(run.fork_level or 0 for run in runs)
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:

        for level in range(max_level + 1):
            runs_to_log = [run for run in runs if (run.fork_level or 0) == level]
            futures = []
            for run in runs_to_log:
                futures.append(executor.submit(log_run, run, e2e_alpha_project))

            for f in futures:
                f.result()
