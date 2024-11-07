import math
import os
import random

from neptune_scale import Run

# Populate an EMPTY project with data used for e2e tests. The shape
# of the data is deterministic, so we can easily test fetching.
#
# The following environment variables are required:
#    - NEPTUNE_PROJECT: the project to populate, it should be empty
#    - NEPTUNE_API_TOKEN: the API token to use
#
# The data consists of:
#     - 6 runs
#        - custom_run_id and family id: id-run-1...id-run-10
#        - runs 1-5 are tagged [head, tag1]
#        - runs 5-10 are tagged [tail, tag1]
#     - 6 experiments
#        - custom_run_id and family id: id-exp-1...id-exp-10
#        - named exp1, exp2... etc
#        - experiments 1-5 are tagged [head, tag2]
#        - experiments 5-10 are tagged [tail, tag2]
#     - each run/experiment attributes:
#       - config/foo1 up to foo10000: string, value = valfoo1, valfoo2 etc
#       - config/bar1 up to bar10000: int, value = 1, 2, 3...
#       - metrics/foo1 up to foo10000: last value is 1, 2 etc
#       - metrics/bar1 up to bar10000: last value is 1, 2, etc
#       - config/foo{number}-unique-{custom_run_id}, from 1 to 100, value is `number`
#       - metrics/bar{number}-unique-{custom_run_id}, from 1 to 100, value is `number`
#       - each metric has 10 steps
#
# Each run thus has:
# - 30 config fields, 10 of which are unique to the run
# - 30 metric fields, 10 of which are unique to the run

MM_NUM_RUNS = 6
MM_NUM_FIELD_KIND = 10
MM_NUM_STEPS = 10


def populate_run(run, run_id, tags=None):
    if tags:
        run.add_tags(tags)

    data = {f"config/foo{x + 1}": f"valfoo{x + 1}" for x in range(MM_NUM_FIELD_KIND)}
    data |= {f"config/bar{x + 1}": x + 1 for x in range(MM_NUM_FIELD_KIND)}
    data |= {f"config/foo{x + 1}-unique-{run_id}": x + 1 for x in range(10)}
    run.log_configs(data)

    step = 0
    for step in range(MM_NUM_STEPS - 1):
        value = math.sin((step + random.random() - 0.5) * 0.1)
        data = {f"metrics/foo{x + 1}": value for x in range(MM_NUM_FIELD_KIND)}
        data |= {f"metrics/bar{x + 1}": value for x in range(MM_NUM_FIELD_KIND)}
        data |= {f"metrics/bar{x + 1}-unique-{run_id}": value for x in range(10)}
        run.log_metrics(step=step, data=data)

    # Last step will have a predetermined value
    step += 1
    data = {f"metrics/foo{x + 1}": x + 1 for x in range(MM_NUM_FIELD_KIND)}
    data |= {f"metrics/bar{x + 1}-unique-{run_id}": x + 1 for x in range(10)}
    data |= {f"metrics/bar{x + 1}": x + 1 for x in range(MM_NUM_FIELD_KIND)}

    run.log_metrics(step=step, data=data)


def populate_many_metrics(project):
    for x in range(MM_NUM_RUNS // 2):
        create_runs(project, x + 1, tags=["head", "tag1"])

    for x in range(MM_NUM_RUNS // 2, MM_NUM_RUNS):
        create_runs(project, x + 1, tags=["tail", "tag1"])

    for x in range(MM_NUM_RUNS // 2):
        create_runs(project, x + 1, tags=["head", "tag2"], experiment_name=f"exp{x + 1}")

    for x in range(MM_NUM_RUNS // 2, MM_NUM_RUNS):
        create_runs(project, x + 1, tags=["tail", "tag2"], experiment_name=f"exp{x + 1}")


def create_runs(project, index, tags, experiment_name=None):
    print(f"create_runs({project}, {index}, {tags}, {experiment_name})")
    kind = "run" if not experiment_name else "exp"
    run_id = f"id-{kind}-{index}"
    with Run(project=project, run_id=run_id, experiment_name=experiment_name) as run:
        print("Populating run", run_id)
        populate_run(run, run_id, tags=tags)


def main():
    populate_many_metrics(os.getenv("NEPTUNE_PROJECT"))


if __name__ == "__main__":
    main()
