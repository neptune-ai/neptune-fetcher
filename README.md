# Neptune Fetcher

!EXPERIMENTAL! Neptune Fetcher is a Python package designed to separate data retrieval capabilities from the regular `neptune` package. This separation bypasses the need to initialize the heavy structures of the regular package, which makes data fetching more efficient and improves performance.

## Installation
```bash
pip install neptune-fetcher
```

## Example usage

1. Fetch the data frame containing Runs fields:
    ```python
    from neptune_fetcher import ReadOnlyProject

    project = ReadOnlyProject('workspace/project')
    # Fetch all runs with specific columns
    runs_df = project.fetch_runs_df(
        columns=["sys/name", "sys/modification_time", "training/lr"],
    )
    ```

2. Fetching data from multiple Runs:
    ```python
    from neptune_fetcher import ReadOnlyProject

    project = ReadOnlyProject('workspace/project')

    for run in project.fetch_read_only_runs(with_ids=["RUN-1", "RUN-2"]):
        run.prefetch(["parameters/optimizer", "parameters/init_lr"])

        print(run["parameters/optimizer"].fetch())
        print(run["parameters/init_lr"].fetch())
    ```

3. Listing Runs identifiers:
    ```python
    from neptune_fetcher import ReadOnlyProject

    project = ReadOnlyProject('workspace/project')

    for run in project.list_runs():
        print(run)
    ```

4. Fetching data from a single Run:
    ```python
    from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

    project = ReadOnlyProject('workspace/project')
    run = ReadOnlyRun(project, with_id='TES-1')
    run.prefetch(["parameters/optimizer", "parameters/init_lr"])

    print(run["parameters/optimizer"].fetch())
    print(run["parameters/init_lr"].fetch())
   ```

## API

### `ReadOnlyProject`

#### Initialization
Initialize with the `ReadOnlyProject` class constructor.

__Parameters__:
- `project`: `str`, optional, default: `None` - Name of a project in the form `workspace-name/project-name`. If None, the value of the `NEPTUNE_PROJECT` environment variable is used.
- `api_token`: `str`, optional, default: `None` - Your Neptune API token (or a service account's API token). If None, the value of the `NEPTUNE_API_TOKEN` environment variable is used. To keep your token secure, avoid placing it in source code. Instead, save it as an environment variable.
- `proxies`: `dict`, optional, default: `None` - Argument passed to HTTP calls made via the Requests library. For details on proxies, see the Requests documentation.

__Example__:
```python
project = ReadOnlyProject('workspace/project', api_token='...')
```

#### `list_runs()`
Lists minimal information like identifier and name for every Run in a project.

__Example__:
```python
for run in project.list_runs():
    print(run)
```

__Returns__:
`Iterator` of dictionaries with Run identifiers and names.

#### `fetch_runs()`
Fetches a table containing IDs and names of runs in the project.

__Example__:
```python
df = project.fetch_runs()
```

__Returns__:
`pandas.DataFrame` with two columns (`sys/id` and `sys/name`) and rows corresponding to project runs.

#### `fetch_runs_df()`
Fetches the runs’ metadata and returns them as a pandas DataFrame.

__Parameters__:
- `columns`: `List[str]`, optional, default: `None` - Names of columns to include in the table, as a list of field names. The Neptune ID (`"sys/id"`) is included automatically. If None, all the columns of the models table are included.
- `with_ids`: `List[str]`, optional, default: `None` - List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.
- `states`: `List[str]`, optional, default: `None` - List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the run. Matching any element of the list is sufficient to pass the criterion.
- `owners`: `List[str]`, optional, default: `None` - List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the run. Matching any element of the list is sufficient to pass the criterion.
- `tags`: `List[str]`, optional, default: `None` - None	A list of tags. Example: "lightGBM" or `["pytorch", "cycleLR"]`. Note: Only runs that have all specified tags will pass this criterion.
- `trashed`: `bool`, optional, default: `False` - Whether to retrieve trashed runs. If True, only trashed runs are retrieved. If False, only non-trashed runs are retrieved. If None or left empty, all run objects are retrieved, including trashed ones.
- `limit`: `int`, optional, default: `None` - Maximum number of runs to fetch. If None, all runs are fetched.
- `sort_by`: `str`, optional, default: `sys/creation_time` - Name of the field to sort the results by. The field must represent a simple type (string, float, integer).
- `ascending`: `bool`, optional, default: `False` - Whether to sort the entries in ascending order of the sorting column values.
- `progress_bar`: `bool`, `Type[ProgressBarCallback]`, optional, default: `None` - Set to `False `to disable the download progress bar, or pass a type of ProgressBarCallback to use your own progress bar. If set to None or True, the default tqdm-based progress bar will be used.

__Example__:
```python
# Fetch all runs with specific columns
runs_df = project.fetch_runs_df(
	columns=["sys/name", "sys/modification_time", "training/lr"],
)

# Fetch runs by specific IDs
specific_runs_df = my_project.fetch_runs_df(
	with_ids=["RUN-123", "RUN-456"]
)
```

__Returns__:
`pandas.DataFrame`: A pandas `DataFrame` containing information about fetched runs.


#### `fetch_read_only_runs()`
List runs of the project in the form of `ReadOnlyRun`.

__Parameters__:
- `with_ids`: `List[str]` - List of Neptune run IDs to fetch.

__Example__:
```python
for run in project.fetch_read_only_runs(with_ids=['RUN-1', 'RUN-2']):
    ...
```

__Returns__:
`Iterator` of `ReadOnlyRun` objects.


### `ReadOnlyRun`

#### Initialization
Could be created as a result of `ReadOnlyProject` method `fetch_read_only_runs` or class constructor.

__Parameters__:
- `read_only_project`: `ReadOnlyProject` - Source project from which run will be fetched.
- `with_id`: `str` - Neptune run ID to fetch. Example: `RUN-1`.

__Example__:
```python
run = ReadOnlyRun(project, with_id='TES-1')
```

#### `field_names`
List of Run field names.

__Example__:
```python
for run in project.fetch_read_only_runs(with_ids=['TES-2', 'TES-1']):
    print(list(run.field_names))
```

__Returns__:
`Iterator` of `str` with Run fields.


#### Field lookup: `run[field_name]`
Allow to access to specific field of Run. See Available types.

__Returns__:
An internal object that allow to operate on specific field. See Available types.

__Example__
```python
run_id = run['sys/id'].fetch()
```

#### `prefetch()`
It allows to fetch a bunch of fields of simple type to the internal cache. Improves the performance of access to consecutive field values.

__Parameters__:
- `paths`: `List[str]` - List of paths to fetch to the cache.

__Example__:
```python
run.prefetch(["parameters/optimizer", "parameter/init_lr"])
# No more calls to the API
print(run["parameters/optimizer"].fetch())
print(run["parameter/init_lr"].fetch())
```

## Available types
Currently supported field types with available data retrieval operations:

### Integer
#### `fetch()`
Retrieves value from either internal cache (see `prefetch()`) or from the API.

__Example__:
```python
my_value = run['value'].fetch()
```
__Returns__:
`int`

### Float
#### `fetch()`
Retrieves value from either internal cache (see `prefetch()`) or from the API.

__Example__:
```python
my_value = run['value'].fetch()
```
__Returns__:
`float`

### String
#### `fetch()`
Retrieves value from either internal cache (see `prefetch()`) or from the API.

__Example__:
```python
my_value = run['value'].fetch()
```

__Returns__:
`str`


## License

This project is licensed under the Apache License Version 2.0. For more details, see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
