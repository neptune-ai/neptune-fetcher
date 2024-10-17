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

* `NEPTUNE_PROJECT` - project name to use. It is set by the test runner script in
  GitHub setup.
* `NEPTUNE_API_TOKEN` - API token to use

Test that populate data during execution could interfere with those that rely on a
specific data shape. To avoid this, set `NEPTUNE_E2E_PROJECT` to a different project
that already exists.

If the env variable `NEPTUNE_E2E_CUSTOM_RUN_ID` is set, it should be `sys/custom_run_id`
of an existing Run. This avoids creating a new Run for tests that log data, if this
is for some reason required.
