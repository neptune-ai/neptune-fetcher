# Neptune Fetcher

> [!NOTE]
> This package is **experimental**.

Neptune Fetcher is a Python package designed to separate data retrieval capabilities from the regular `neptune` package. This separation bypasses the need to initialize the heavy structures of the regular package, which makes data fetching more efficient and improves performance.

## Installation

```bash
pip install neptune-fetcher
```

## Example usage

### Fetching data frame containing specified fields as columns

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")
# Fetch all runs with specific columns
runs_df = project.fetch_runs_df(
    columns=["sys/name", "sys/modification_time"],
    columns_regex="tree/.*",  # added to columns specified with the "columns" parameter
)
```

### Fetching data from specified runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for run in project.fetch_read_only_runs(with_ids=["RUN-1", "RUN-2"]):
    run.prefetch(["parameters/optimizer", "parameters/init_lr"])

    print(run["parameters/optimizer"].fetch())
    print(run["parameters/init_lr"].fetch())
```

### Listing run identifiers

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for run in project.list_runs():
    print(run)
```

### Fetching data from a single run

```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

project = ReadOnlyProject("workspace/project")
run = ReadOnlyRun(project, with_id="TES-1")
run.prefetch(["parameters/optimizer", "parameters/init_lr"])

print(run["parameters/optimizer"].fetch())
print(run["parameters/init_lr"].fetch())
```

## API reference

### `ReadOnlyProject`

Representation of a Neptune project in a limited read-only mode.

#### Initialization

Initialize with the ReadOnlyProject class constructor.

__Parameters__:

| Name        | Type             | Default | Description                                                                                                                                                                                                                                       |
|-------------|------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `project`   | `str`, optional  | `None`  | Name of a project in the form `workspace-name/project-name`. If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used.                                                                                                          |
| `api_token` | `str`, optional  | `None`  | Your Neptune API token (or a service account's API token). If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. To keep your token secure, avoid placing it in source code. Instead, save it as an environment variable. |
| `proxies`   | `dict`, optional | `None`  | Argument passed to HTTP calls made via the Requests library. For details on proxies, see the [Requests documentation](https://requests.readthedocs.io/).                                                                                          |

__Example__:

```python
project = ReadOnlyProject("workspace/project", api_token="...")
```

---

#### `list_runs()`

Lists minimal information, like identifier and name, for every run in a project.

__Example__:
```python
for run in project.list_runs():
    print(run)
```

__Returns__:
`Iterator` of dictionaries with Neptune run identifiers, custom identifiers and names.

---

#### `fetch_runs()`

Fetches a table containing Neptune IDs, custom run IDs and names of runs in the project.

__Example__:
```python
df = project.fetch_runs()
```

__Returns__:
`pandas.DataFrame` with three columns (`sys/id`, `sys/name` and `sys/custom_run_id`) and rows corresponding to project runs.

---

#### `fetch_runs_df()`

Fetches the runs' metadata and returns them as a pandas DataFrame.

__Parameters__:

| Name              | Type                                          | Default             | Description                                                                                                                                                                                                                                                                            |
|-------------------|-----------------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `columns`         | `List[str]`, optional                         | `None`              | Names of columns to include in the table, as a list of field names. The Neptune ID (`"sys/id"`) is included automatically. If `None`, all the columns of the experiments table are included.                                                                                           |
| `columns_regex`   | `str`, optional                               | `None`              | A regex pattern to filter columns by name. Use this parameter to include columns in addition to the ones specified by the `columns` parameter.                                                                                                                                         |
| `names_regex`     | `str`, optional                               | `None`              | A regex pattern to filter the runs by name. When applied, it needs to limit the number of runs to 100 or fewer.                                                                                                                                                                        |
| `custom_id_regex` | `str`, optional                               | `None`              | A regex pattern to filter the runs by custom ID. When applied, it needs to limit the number of runs to 100 or fewer.                                                                                                                                                                   |
| `with_ids`        | `List[str]`, optional                         | `None`              | List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                     |
| `custom_ids` | `List[str]`, optional                         | `None`              | List of multiple Custom IDs. Example: `["nostalgic_shockley", "high_albattani"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                |
| `states`          | `List[str]`, optional                         | `None`              | List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the run. Matching any element of the list is sufficient to pass the criterion.                                                                                     |
| `owners`          | `List[str]`, optional                         | `None`              | List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the run. Matching any element of the list is sufficient to pass the criterion.                                                                                                            |
| `tags`            | `List[str]`, optional                         | `None`              | A list of tags. Example: `"lightGBM"` or `["pytorch", "cycleLR"]`. **Note:** Only runs that have all specified tags will pass this criterion.                                                                                                                                          |
| `trashed`         | `bool`, optional                              | `False`             | Whether to retrieve trashed runs. If `True`, only trashed runs are retrieved. If `False`, only non-trashed runs are retrieved. If `None` or left empty, all run objects are retrieved, including trashed ones.                                                                         |
| `limit`           | `int`, optional                               | `None`              | Maximum number of runs to fetch. If `None`, all runs are fetched.                                                                                                                                                                                                                      |
| `sort_by`         | `str`, optional                               | `sys/creation_time` | Name of the field to sort the results by. The field must represent a simple type (string, float, integer).                                                                                                                                                                             |
| `ascending`       | `bool`, optional                              | `False`             | Whether to sort the entries in ascending order of the sorting column values.                                                                                                                                                                                                           |
| `progress_bar`    | `bool`, `Type[ProgressBarCallback]`, optional | `None`              | Set to `False `to disable the download progress bar, or pass a type of ProgressBarCallback to [use your own progress bar](https://docs.neptune.ai/usage/querying_metadata/#using-a-custom-progress-bar). If set to `None` or `True`, the default tqdm-based progress bar will be used. |

__Example__:
```python
# Fetch all runs with specific columns
runs_df = project.fetch_runs_df(
    columns=["sys/name", "sys/modification_time", "training/lr"]
)

# Fetch all runs with specific columns and extra columns that match a regex pattern
runs_df = project.fetch_runs_df(
    columns=["sys/name", "sys/modification_time"],
    columns_regex="tree/.*",
)

# Fetch runs by specific IDs
specific_runs_df = my_project.fetch_runs_df(
    custom_ids=["nostalgic_shockley", "high_albattani"]
)

# Filter by name regex
specific_runs_df = my_project.fetch_runs_df(
    names_regex="tree_3[2-4]+"
)
```

__Returns__:
`pandas.DataFrame`: A pandas DataFrame containing metadata of the fetched runs.

---

#### `fetch_read_only_runs()`
List runs of the project in the form of ReadOnlyRun.

__Parameters__:

| Name         | Type                  | Default | Description                       |
|--------------|-----------------------|---------|-----------------------------------|
| `with_ids`   | `Optional[List[str]]` | `None`  | List of Neptune run IDs to fetch. |
| `custom_ids` | `Optional[List[str]]` | `None`  | List of custom run IDs to fetch.  |

__Example__:
```python
for run in project.fetch_read_only_runs(custom_ids=["my-run-aa-1", "my-run-bb-2"]):
    ...
```

__Returns__:
Iterator of ReadOnlyRun objects.

---

### `ReadOnlyRun`

Representation of a Neptune run in a limited read-only mode.

#### Initialization

Can be created with the class constructor, or as a result of the [`fetch_read_only_runs()`](#fetch_read_only_runs) method of the ReadOnlyProject class.

__Parameters__:

| Name                | Type              | Default | Description                                                                   |
|---------------------|-------------------|---------|-------------------------------------------------------------------------------|
| `read_only_project` | `ReadOnlyProject` | -       | Source project from which run will be fetched.                                |
| `with_id`           | `Optional[str]`   | `None`  | Neptune run ID to fetch. Example: `RUN-1`. Exclusive with `custom_id`         |
| `custom_id`         | `Optional[str]`   | `None`  | Neptune Custom IDs to fetch. Example: `my-run-aa-1`. Exclusive with `with_id` |

__Example__:
```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

project = ReadOnlyProject("workspace/project", api_token="...")
run = ReadOnlyRun(project, with_id="TES-1")
```

---

#### `.field_names`
List of run field names.

__Example__:
```python
for run in project.fetch_read_only_runs(with_ids=["TES-1", "TES-2"]):
    print(list(run.field_names))
```

__Returns__:
Iterator of run fields as strings.


---

#### Field lookup: `run[field_name]`
Used to access a specific field of a run. See [Available types](#available-types).

__Returns__:
An internal object used to operate on a specific field.

__Example__:
```python
custom_id = run["sys/custom_run_id"].fetch()
```

---

#### `prefetch()`
Pre-fetches a batch of fields to the internal cache.

Improves the performance of access to consecutive field values. Only simple field types are supported (`int`, `float`, `str`).

__Parameters__:

| Name    | Type        | Default | Description                          |
|---------|-------------|---------|--------------------------------------|
| `paths` | `List[str]` | -       | List of paths to fetch to the cache. |

__Example__:
```python
run.prefetch(["parameters/optimizer", "parameter/init_lr"])
# No more calls to the API
print(run["parameters/optimizer"].fetch())
print(run["parameter/init_lr"].fetch())
```

## Available types

The following sections list the currently supported field types, along with their available data retrieval operations.

---

### Integer
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
batch_size = run["batch_size"].fetch()
```
__Returns__:
`int`

---

### Float
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
f1 = run["scores/f1"].fetch()
```
__Returns__:
`float`

---

### String
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
token = run["token"].fetch()
```

__Returns__:
`str`

---

### Datetime
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
created_at = run["sys/creation_time"].fetch()
```

__Returns__:
`datetime.datetime`

---

### Object state
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
state = run["sys/state"].fetch()
```

__Returns__:
`str`

---

### Boolean
#### `fetch()`
Retrieves value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
status = run["sys/failed"].fetch()
```

__Returns__:
`bool`

---

### Float series
#### `fetch()` or `fetch_last()`
Retrieves last series value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example__:
```python
loss = run["loss"].fetch_last()
```

__Returns__:
`Optional[float]`

#### `fetch_values()`
Retrieves all series values from the API.

__Parameters__:

| Name                | Type   | Default | Description                                                  |
|---------------------|--------|---------|--------------------------------------------------------------|
| `include_timestamp` | `bool` | True    | Whether the fetched data should include the timestamp field. |

__Example__:
```python
values = run["loss"].fetch_values()
```

__Returns__:
`pandas.DataFrame`

---

## License

This project is licensed under the Apache License Version 2.0. For more details, see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
