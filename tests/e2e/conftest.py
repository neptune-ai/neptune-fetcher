import os
import uuid
from datetime import (
    datetime,
    timezone,
)

from neptune_scale import Run
from pytest import fixture

from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)

# NOTE
# The fixtures below assume that we're testing on a "many metrics" project
# populated using the tests/populate_projects.py script, and that the project
# contains ONLY data generated by the script.
# If this is not the case, the tests will fail, as there are hard assumptions
# around names and values of the metadata.


@fixture(scope="module")
def project(request):
    # Assume the project name and API token are set in the environment using the standard
    # NEPTUNE_PROJECT and NEPTUNE_API_TOKEN variables.
    #
    # Since ReadOnlyProject is essentially stateless, we can reuse the same
    # instance across all tests in a module.
    #
    # We also allow overriding the project name per module by setting the
    # module-level `NEPTUNE_PROJECT` variable.
    project_name = getattr(request.module, "NEPTUNE_PROJECT", None)
    return ReadOnlyProject(project=project_name)


@fixture
def all_run_ids():
    return sorted([f"id-run-{i + 1}" for i in range(6)])


@fixture
def all_experiment_ids():
    return sorted([f"id-exp-{i + 1}" for i in range(6)])


@fixture
def all_experiment_names():
    return sorted([f"exp{i + 1}" for i in range(6)])


@fixture
def sys_columns_set(sys_columns):
    return set(sys_columns)


@fixture
def sys_columns():
    return ["sys/id", "sys/name", "sys/custom_run_id"]


@fixture
def id_to_name():
    """Expected sys/custom_run_id -> sys/name"""
    d = {f"id-run-{num}": "" for num in range(1, 7)}
    d |= {f"id-exp-{num}": f"exp{num}" for num in range(1, 7)}

    return d


class SyncRun(Run):
    """A neptune_scale.Run instance that waits for processing to complete
    after each logging method call. This is useful for e2e tests, where we
    usually want to wait for the data to be available before fetching it."""

    def log(self, *args, **kwargs):
        result = super().log(*args, **kwargs)
        self.wait_for_processing()
        return result


@fixture(scope="module")
def run_init_kwargs(project):
    """Arguments to initialize a neptune_scale.Run instance"""

    # TODO: if a test fails the run could be left in an indefinite state
    #       Maybe we should just have it scoped 'function' and require passing
    #       an existing run id
    kwargs = {"project": project.project_identifier}
    run_id = os.getenv("NEPTUNE_E2E_CUSTOM_RUN_ID")
    if run_id is None:
        run_id = str(uuid.uuid4())
        kwargs["experiment_name"] = "pye2e-fetcher"
    else:
        kwargs["resume"] = True

    kwargs["run_id"] = run_id

    return kwargs


@fixture(scope="module")
def run(project, run_init_kwargs):
    """Plain neptune_scale.Run instance. We're scoping it to "module", as it seems to be a
    good compromise, mostly because of execution time."""

    run = Run(**run_init_kwargs)
    run.log_configs({"test_start_time": datetime.now(timezone.utc)})

    return run


@fixture(scope="module")
def sync_run(project, run_init_kwargs):
    """Blocking run for logging data"""
    return SyncRun(**run_init_kwargs)


@fixture
def ro_run(run, project):
    """ReadOnlyRun pointing to the same run as the neptune_scale.Run"""
    return ReadOnlyRun(read_only_project=project, custom_id=run._run_id)
