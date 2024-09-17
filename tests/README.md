# Important note

All tests in the `e2e` directory are end-to-end tests that assume a
specific data layout in a project.

This data is populated using the `populate_projects.py` script, which
has comments explaining the layout.

The script *must* be run only **once** on a fresh project to set up the
necessary data for the tests to run successfully.
