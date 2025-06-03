# Important notes

## Prepopulated test data

A subset of the tests in the `e2e` directory are end-to-end tests that assume a
specific data layout in a project.

This data is populated using the `populate_projects.py` script, which
has comments explaining the layout.

The script *must* be run only **once** on a fresh project to set up the
necessary data for the tests to run successfully.

The tests that rely on this data are:

* `test_dataframe_values.py`
* `test_run_filtering.py`
* `test_run_list.py`

## Environment variables

* `NEPTUNE_API_TOKEN` - API token to use
* `NEPTUNE_E2E_PROJECT_PREPOPULATED` - project name to use for tests that require a project
  with fixed data populated by `populate_projects.py` and **do not** populate the
  project during execution. The test runner script creates a temporary project for
  that purpose. If not set, `NEPTUNE_PROJECT` is used.
* `NEPTUNE_E2E_PROJECT` - project name to use for tests that create Runs during
  execution. This can be an existing project in which it's fine to create multiple
  Runs with different data. If not set, `NEPTUNE_PROJECT` is used.
* `NEPTUNE_E2E_CUSTOM_RUN_ID` (optional) - if set, it should be `sys/custom_run_id`
  of an existing Run. This avoids creating a new Run for tests that log data,
  if this is for some reason required.
