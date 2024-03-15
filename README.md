# Neptune Fetcher

!EXPERIMENTAL! Neptune Fetcher is a Python package designed for efficient fetching and manipulation of data from Neptune projects and runs. It provides classes and methods for interacting with Neptune data in a read-only manner.

## Installation
```bash
pip install neptune-fetcher
```

## Usage

### Importing

```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun
```

### Overview of Classes
- `ReadOnlyProject`: A lightweight, read-only class for handling basic project information.
    - _Constructor Parameters_:
        - `project`: Optional string specifying the project name.
        - `workspace`: Optional string specifying the workspace.
        - `api_token`: Optional string for the API token.
        - `proxies`: Optional dictionary for proxy configuration.
    - _Methods_:
        - `list_runs()`: Yields dictionaries with basic information about each run, including `sys/id` and `sys/name`.
        - `fetch_read_only_runs(with_ids: List[str])`: Returns a generator for `ReadOnlyRun` instances for specified run IDs.
        - `fetch_runs()`: Fetches runs as a DataFrame with default columns.
        - `fetch_runs_df()`: Fetches runs as a DataFrame based on specified filters.

- _`ReadOnlyRun`_: Represents a single Neptune run with read-only access.
    - _Methods_:
        - `__getitem__()`: Accesses a field by its path.
        - `field_names`: Yields the names of all available fields in the run.
        - `prefetch()`: Loads values of specified fields into local cache.


## Examples
### Fetching Project Metadata

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
```

### Listing Runs in a Project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
ids = list(map(lambda row: row["sys/id"], project.list_runs()))
```

### Filtering and Processing Runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
df = project.fetch_runs_df()

matches = df["sys/name"].str.match("metrics.*")
ids = df[matches]["sys/id"]
```

### Iterating Over Runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject(workspace="some", project="project")
for run in project.fetch_read_only_runs(with_ids=["PROJ-2"]):
    for field in run.field_names:
        if field.startswith("param"):
            print(run[field].fetch())
        if field.startswith("metric"):
            print(run[field].fetch_values())
```

### Prefetching Values

```python
run.prefetch(["metric1", "metric2"])
print(run["metric1"].fetch(), run["metric2"].fetch())  # This will use the local cache
```

### Purging Local Cache

```python
del run["metric1"]
```


### Example
A full example can be found in `examples/fetch_api.py`.

## License

This project is licensed under the Apache License Version 2.0. For more details, see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
