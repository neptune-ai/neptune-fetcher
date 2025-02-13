import concurrent.futures
from dataclasses import dataclass
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from typing import (
    Optional,
    Union,
)

from neptune_scale import Run

AttributeName = str
Step = float
Value = float

FIXED_DATE = datetime(2025, 2, 10, 12, 0, 0, 0, timezone.utc)


@dataclass(frozen=True)
class GeneratedRun:
    custom_run_id: str
    experiment_name: str
    fork_run_id: Union[str, None]
    fork_point: Optional[int]
    configs: dict[AttributeName, Union[float, bool, int, str, datetime, list, set, tuple]]
    metrics: dict[AttributeName, dict[Step, Value]]
    tags: list[str]

    def attributes(self):
        return set().union(self.configs.keys(), self.metrics_attributes())

    def metrics_attributes(self):
        if self.fork_run_id:
            return set().union(self.metrics.keys(), RUNS_BY_ID[self.fork_run_id].metrics_attributes())
        return set(self.metrics.keys())

    def metrics_values(self, name: AttributeName) -> list[tuple[Step, Value]]:
        return list(self.metrics[name].items())


# Tree structure:
#
# linear_history_tree:
#
# root (level: None)
#   └── fork1 (level: 1, fork_point: 4)
#         └── fork2 (level: 2, fork_point: 8)
#

LINEAR_TREE_EXP_NAME = "exp_with_linear_history"
LINEAR_HISTORY_TREE = [
    GeneratedRun(
        custom_run_id="linear_history_root",
        experiment_name=LINEAR_TREE_EXP_NAME,
        fork_point=None,
        fork_run_id=None,
        tags=["linear_root", "linear"],
        configs={
            "int-value": 1,
            "float-value": 1.0,
            "str-value": "hello_1",
            "linear-history": True,
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.1 for step in range(10)},
            "foo1": {step: step * 0.2 for step in range(10)},
            "unique1/0": {step: step * 0.3 for step in range(10)},
        },
    ),
    GeneratedRun(
        custom_run_id="linear_history_fork1",
        fork_run_id="linear_history_root",
        experiment_name=LINEAR_TREE_EXP_NAME,
        fork_point=4,
        tags=["linear_fork1", "linear"],
        configs={
            "int-value": 2,
            "float-value": 2.0,
            "str-value": "hello_2",
            "linear-history": True,
            "bool-value": True,
            "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.4 for step in range(5, 10)},
            "foo1": {step: step * 0.5 for step in range(5, 10)},
            "unique2/0": {step: step * 0.6 for step in range(5, 10)},
        },
    ),
    GeneratedRun(
        custom_run_id="linear_history_fork2",
        fork_run_id="linear_history_fork1",
        experiment_name=LINEAR_TREE_EXP_NAME,
        fork_point=8,
        tags=["linear_fork2", "linear"],
        configs={
            "int-value": 3,
            "float-value": 3.0,
            "str-value": "hello_3",
            "linear-history": True,
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.7 for step in range(9, 20)},
            "foo1": {step: step * 0.8 for step in range(9, 20)},
            "unique3/0": {step: step * 0.9 for step in range(9, 20)},
        },
    ),
]

# Tree structure:
#
# forked_history_tree:
#
# root (level: None)
#   ├── fork1 (level: 1, fork_point: 4)
#   └── fork2 (level: 1, fork_point: 8)

FORKED_TREE_EXP_NAME = "epx_with_forked_history"

FORKED_HISTORY_TREE = [
    GeneratedRun(
        custom_run_id="forked_history_root",
        experiment_name=FORKED_TREE_EXP_NAME,
        fork_point=None,
        fork_run_id=None,
        tags=["forked_history_root", "forked_history"],
        configs={
            "int-value": 1,
            "float-value": 1.0,
            "str-value": "hello_1",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.1 for step in range(1, 5)},
            "foo1": {step: step * 0.2 for step in range(1, 5)},
            "unique1/0": {step: step * 0.3 for step in range(1, 5)},
        },
    ),
    GeneratedRun(
        custom_run_id="forked_history_fork1",
        experiment_name=FORKED_TREE_EXP_NAME,
        fork_point=4,
        fork_run_id="forked_history_root",  # References root
        tags=["forked_history_fork1", "forked_history"],
        configs={
            "int-value": 2,
            "float-value": 2.0,
            "str-value": "hello_2",
            "bool-value": True,
            "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.4 for step in range(5, 9)},
            "foo1": {step: step * 0.5 for step in range(5, 9)},
            "unique2/0": {step: step * 0.6 for step in range(5, 9)},
        },
    ),
    GeneratedRun(
        custom_run_id="forked_history_fork2",
        experiment_name=FORKED_TREE_EXP_NAME,
        fork_point=8,
        fork_run_id="forked_history_root",  # References root
        tags=["forked_history_fork2", "forked_history"],
        configs={
            "int-value": 3,
            "float-value": 3.0,
            "str-value": "hello_3",
            "bool-value": False,
            "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
        },
        metrics={
            "foo0": {step: step * 0.7 for step in range(9, 20)},
            "foo1": {step: step * 0.8 for step in range(9, 20)},
            "unique3/0": {step: step * 0.9 for step in range(9, 20)},
        },
    ),
]

ALL_STATIC_RUNS = LINEAR_HISTORY_TREE + FORKED_HISTORY_TREE
RUNS_BY_ID = {run.custom_run_id: run for run in ALL_STATIC_RUNS}

ALL_STATIC_EXPERIMENTS = [RUNS_BY_ID["forked_history_fork2"], RUNS_BY_ID["linear_history_fork2"]]
EXPERIMENTS_BY_NAME = {exp.experiment_name: exp for exp in ALL_STATIC_EXPERIMENTS}

# For linear history tree we want to log runs that linear_history_fork2 will be last logged (experiment head)
TOPO_SORTED_RUN_IDS = [
    ["linear_history_root", "forked_history_root"],
    ["linear_history_fork1", "forked_history_fork1"],
    ["linear_history_fork2", "forked_history_fork2"],
]


def timestamp_for_step(step: int):
    return FIXED_DATE + timedelta(minutes=step)


def log_run(generated: GeneratedRun, e2e_alpha_project: str):
    with Run(
        project=e2e_alpha_project,
        run_id=generated.custom_run_id,
        experiment_name=generated.experiment_name,
        fork_run_id=generated.fork_run_id,
        fork_step=generated.fork_point,
    ) as run:
        run.log_configs(data=generated.configs)
        run.add_tags(generated.tags)
        for metric_name, metric_values in generated.metrics.items():
            for step, value in metric_values.items():
                run.log_metrics(step=step, data={metric_name: value}, timestamp=timestamp_for_step(step))


def log_runs(e2e_alpha_project: str, runs: list[GeneratedRun]):
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for runs_to_log in TOPO_SORTED_RUN_IDS:
            futures = []
            for run in runs_to_log:
                futures.append(executor.submit(log_run, RUNS_BY_ID[run], e2e_alpha_project))

            for f in futures:
                f.result()
