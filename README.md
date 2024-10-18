# Neptune Fetcher

> [!NOTE]
> This package is experimental and only works with Neptune Scale, which is in beta.
>
> You can't use this package with `neptune<2.0` or the currently available Neptune app version. For the corresponding
> Python API, see [neptune-client](https://github.com/neptune-ai/neptune-client).

Neptune Fetcher is designed to separate data retrieval capabilities from the regular `neptune` package. This separation
makes data fetching more efficient and improves performance.

## Installation

```bash
pip install neptune-fetcher
```

## Example usage

### Listing runs of a project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for run in project.list_runs():
    print(run)  # dicts with identifiers
```

### Listing experiments of a project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for experiment in project.list_experiments():
    print(experiment)  # dicts with identifiers
```

### Fetching runs data frame with specific columns

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

runs_df = project.fetch_runs_df(
    columns=["sys/custom_run_id", "sys/modification_time"],
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

### Fetching data from a single run

```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

project = ReadOnlyProject("workspace/project")
run = ReadOnlyRun(project, with_id="TES-1")

run.prefetch(["parameters/optimizer", "parameters/init_lr"])
run.prefetch_series_values(["metrics/loss", "metrics/accuracy"], use_threads=True)

print(run["parameters/optimizer"].fetch())
print(run["parameters/init_lr"].fetch())
print(run["metrics/loss"].fetch_values())
print(run["metrics/accuracy"].fetch_values())
```

## API reference

### `ReadOnlyProject`

Representation of a Neptune project in a limited read-only mode.

#### Initialization

Initialize with the ReadOnlyProject class constructor:

```python
project = ReadOnlyProject("workspace/project", api_token="...")
```

> [!TIP]
> Find your API token in your user menu, in the bottom-left corner of the Neptune app.

__Parameters:__

| Name        | Type             | Default | Description                                                                                                                                                                                                                                       |
|-------------|------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `project`   | `str`, optional  | `None`  | Name of a project in the form `workspace-name/project-name`. If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used.                                                                                                          |
| `api_token` | `str`, optional  | `None`  | Your Neptune API token (or a service account's API token). If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. To keep your token secure, avoid placing it in source code. Instead, save it as an environment variable. |
| `proxies`   | `dict`, optional | `None`  | Dictionary of proxy settings, if needed. This argument is passed to HTTP calls made via the Requests library. For details on proxies, see the [Requests documentation](https://requests.readthedocs.io/).                                         |

---

#### `list_runs()`

Lists all runs of a project.

Each run is identified by Neptune ID (`sys/id`), custom ID (`sys/custom_run_id`) and, if set, name (`sys/name`).

__Returns:__ `Iterator` of dictionaries with Neptune run identifiers, custom identifiers and names.

__Example:__

```python
project = ReadOnlyProject()

for run in project.list_runs():
    print(run)
```

---

#### `list_experiments()`

Lists all experiments of a project.

Each experiment is identified by:

- Neptune ID: `sys/id`
- (If set) Custom ID: `sys/custom_run_id`
- Name: `sys/name`

__Example:__

```python
for experiment in project.list_experiments():
    print(experiment)
```

__Returns:__ `Iterator` of dictionaries with Neptune experiment identifiers, custom identifiers and names.

---

#### `fetch_runs()`

Fetches a table containing Neptune IDs, custom run IDs and names of runs in the project.

__Returns:__ `pandas.DataFrame` `pandas.DataFrame` with three columns (`sys/id`, `sys/name` and `sys/custom_run_id`)
and one row for each run.

__Example:__

```python
project = ReadOnlyProject()
df = project.fetch_runs()
```

---

#### `fetch_experiments()`

Fetches a table containing Neptune IDs, custom IDs and names of experiments in the project.

__Example__:

```python
df = project.fetch_experiments()
```

__Returns__:
`pandas.DataFrame` with three columns (`sys/id`, `sys/custom_run_id`, `sys/name`) and one row for each experiment.

---

#### `fetch_runs_df()`

Fetches the runs' metadata and returns them as a pandas DataFrame.

__Parameters:__

| Name                       | Type                                          | Default             | Description                                                                                                                                                                                                                                                                                                                                        |
|----------------------------|-----------------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `columns`                  | `List[str]`, optional                         | `None`              | Names of columns to include in the table, as a list of field names. The custom run identifier (`sys/custom_run_id`) is always included. If `None`, only the custom ID and the sorting column are included.
| `columns_regex`            | `str`, optional                               | `None`              | A regex pattern to filter columns by name. Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
| `names_regex`              | `str`, optional                               | `None`              | A regex pattern to filter the runs by name.                                                                                                                                                                                                                                                                                                        |
| `custom_id_regex`          | `str`, optional                               | `None`              | A regex pattern to filter the runs by custom ID.                                                                                                                                                                                                                                                                                                   |
| `with_ids`                 | `List[str]`, optional                         | `None`              | List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                                                 |
| `custom_ids`               | `List[str]`, optional                         | `None`              | List of multiple custom IDs. Example: `["nostalgic_shockley", "high_albattani"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                            |
| `states`                   | `List[str]`, optional                         | `None`              | List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the run. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                 |
| `owners`                   | `List[str]`, optional                         | `None`              | List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the run. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                        |
| `tags`                     | `List[str]`, optional                         | `None`              | A list of tags. Example: `"lightGBM"` or `["pytorch", "cycleLR"]`. **Note:** Only runs that have all specified tags will pass this criterion.                                                                                                                                                                                                      |
| `trashed`                  | `bool`, optional                              | `False`             | Whether to retrieve trashed runs. If `True`, only trashed runs are retrieved. If `False`, only non-trashed runs are retrieved. If `None` or left empty, all run objects are retrieved, including trashed ones.                                                                                                                                     |
| `limit`                    | `int`, optional                               | `None`              | Maximum number of runs to fetch. If `None`, all runs are fetched.                                                                                                                                                                                                                                                                                  |
| `sort_by`                  | `str`, optional                               | `sys/creation_time` | Name of the field to sort the results by. The field must represent a simple type (string, float, integer).                                                                                                                                                                                                                                         |
| `ascending`                | `bool`, optional                              | `False`             | Whether to sort the entries in ascending order of the sorting column values.                                                                                                                                                                                                                                                                       |
| `progress_bar`             | `bool`, `Type[ProgressBarCallback]`, optional | `None`              | Set to `False `to disable the download progress bar, or pass a type of ProgressBarCallback to [use your own progress bar](https://docs.neptune.ai/usage/querying_metadata/#using-a-custom-progress-bar). If set to `None` or `True`, the default tqdm-based progress bar will be used.                                                             |
| `query`                    | `str`, optional                               | `None`              | NQL query string. Example: `"(accuracy: float > 0.88) AND (loss: float < 0.2)"`. Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters. For syntax, see [Neptune Query Language](https://docs.neptune.ai/usage/nql/) in Neptune docs.                                                                             |                                                                                                                  |
| `match_columns_to_filters` | `bool`, optional                              | `True`              | DEPRECATED: The argument is assumed to be `True` and will be removed in a future release.<br/><br/>If `True`, the columns regex only matches against fields that are present in the runs that pass the run filters. The run filters must match no more than 5000 entries. If `False`, the columns regex matches against all fields in the project. |

__Returns:__ `pandas.DataFrame`: A pandas DataFrame containing metadata of the fetched runs.

> [!IMPORTANT]
> The following fields are always included:
>
> - `sys/custom_run_id`: the custom run identifier.
> - The field to sort by. That is, the field name passed to the `sort_by` argument.
>
> The maximum number of runs that can be returned is 5000.


__Examples:__

Fetch all runs, with specific columns:

```python
project = ReadOnlyProject()

runs_df = project.fetch_runs_df(
    columns=["sys/modification_time", "training/lr"]
)
```

Fetch all runs, with specific columns and extra columns that match a regex pattern:

```python
runs_df = project.fetch_runs_df(
    columns=["sys/modification_time"],
    columns_regex="tree/.*",
)
```

Fetch runs by specific ID:

```python
specific_runs_df = my_project.fetch_runs_df(custom_ids=["nostalgic_shockley", "high_albattani"])
```

Fetch runs by names that match a regex pattern:

```python
specific_runs_df = my_project.fetch_runs_df(
    names_regex="tree_3[2-4]+"
)
```

Fetch runs with a complex query:

```python
runs_df = my_project.fetch_runs_df(query="(accuracy: float > 0.88) AND (loss: float < 0.2)")
```

---

#### `fetch_experiments_df()`

Fetches the experiments' metadata and returns them as a pandas DataFrame.

__Parameters__:

| Name                       | Type                                          | Default             | Description                                                                                                                                                                                                                                                                                                                                        |
|----------------------------|-----------------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `columns`                  | `List[str]`, optional                         | `None`              | Names of columns to include in the table, as a list of field names. The sorting column, custom run identifier (`sys/custom_run_id`), and experiment name (`sys/name`) are always included. `None` results in returning only the default columns.
| `columns_regex`            | `str`, optional                               | `None`              | A regex pattern to filter columns by name. Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
| `names_regex`              | `str`, optional                               | `None`              | A regex pattern to filter the experiments by name.                                                                                                                                                                                                                                                                                                 |
| `names_regex`              | `str`, optional                               | `None`              | A regex pattern or a list of regex patterns to filter the experiments by name. Multiple patterns will be connected by AND logic.                                                                                                                                                                                                                   |
| `names_exclude_regex`      | `str`, optional                               | `None`              | A regex pattern or a list of regex patterns to exclude experiments by name. Multiple patterns will be connected by AND logic.                                                                                                                                                                                                                      |
| `custom_id_regex`          | `str`, optional                               | `None`              | A regex pattern to filter the experiments by custom ID.                                                                                                                                                                                                                                                                                            |
| `with_ids`                 | `List[str]`, optional                         | `None`              | List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                                                 |
| `custom_ids`               | `List[str]`, optional                         | `None`              | List of multiple custom IDs. Example: `["nostalgic_shockley", "high_albattani"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                            |
| `states`                   | `List[str]`, optional                         | `None`              | List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the experiment. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                          |
| `owners`                   | `List[str]`, optional                         | `None`              | List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the experiement. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                |
| `tags`                     | `List[str]`, optional                         | `None`              | A list of tags. Example: `"lightGBM"` or `["pytorch", "cycleLR"]`. **Note:** Only experiments that have all specified tags will pass this criterion.                                                                                                                                                                                               |
| `trashed`                  | `bool`, optional                              | `False`             | Whether to retrieve trashed experiments. If `True`, only trashed experiments are retrieved. If `False`, only non-trashed experiments are retrieved. If `None` or left empty, all experiment objects are retrieved, including trashed ones.                                                                                                         |
| `limit`                    | `int`, optional                               | `None`              | Maximum number of experiments to fetch. If `None`, all experiments are fetched.                                                                                                                                                                                                                                                                    |
| `sort_by`                  | `str`, optional                               | `sys/creation_time` | Name of the field to sort the results by. The field must represent a simple type (string, float, integer).                                                                                                                                                                                                                                         |
| `ascending`                | `bool`, optional                              | `False`             | Whether to sort the entries in ascending order of the sorting column values.                                                                                                                                                                                                                                                                       |
| `progress_bar`             | `bool`, `Type[ProgressBarCallback]`, optional | `None`              | Set to `False `to disable the download progress bar, or pass a type of ProgressBarCallback to [use your own progress bar](https://docs.neptune.ai/usage/querying_metadata/#using-a-custom-progress-bar). If set to `None` or `True`, the default tqdm-based progress bar will be used.                                                             |
| `query`                    | `str`, optional                               | `None`              | NQL query string. Example: `"(accuracy: float > 0.88) AND (loss: float < 0.2)"`. Exclusive with the `with_ids`, `custom_ids`, `states`, `owners`, and `tags` parameters. For syntax, see [Neptune Query Language](https://docs.neptune.ai/usage/nql/) in Neptune docs.                                                                             |                                                                                                                  |
| `match_columns_to_filters` | `bool`, optional                              | `True`              | DEPRECATED: The argument is assumed to be `True` and will be removed in a future release.<br/><br/>If `True`, the columns regex only matches against fields that are present in the runs that pass the run filters. The run filters must match no more than 5000 entries. If `False`, the columns regex matches against all fields in the project. |

__Returns:__ `pandas.DataFrame`: A pandas DataFrame containing metadata of the fetched experiments.

> [!IMPORTANT]
> The following fields are always included:
>
> - `sys/custom_run_id`: the custom run identifier.
> - `sys/name`: the experiment name.
> - The field to sort by. That is, the field name passed to the `sort_by` argument.
>
> The maximum number of runs that can be returned is 5000.

__Examples:__

Fetch all experiments with specific columns:

```python
experiments_df = project.fetch_experiments_df(
    columns=["sys/custom_run_id", "sys/modification_time", "training/lr"]
)
```

Fetch all experiments with specific columns and extra columns that match a regex pattern:

```python
experiments_df = project.fetch_experiments_df(
    columns=["sys/custom_run_id", "sys/modification_time"],
    columns_regex="tree/.*",
)
```

Fetch experiments by specific IDs:

```python
specific_experiments_df = my_project.fetch_experiments_df(
    custom_ids=["nostalgic_shockley", "high_albattani"]
)
```

Fetch experiments with a complex query:

```python
experiments_df = my_project.fetch_experiments_df(query="(accuracy: float > 0.88) AND (loss: float < 0.2)")
```

---

#### `fetch_read_only_runs()`

List runs of the project in the form of ReadOnlyRun.

__Parameters:__

| Name         | Type                  | Default | Description                       |
|--------------|-----------------------|---------|-----------------------------------|
| `with_ids`   | `Optional[List[str]]` | `None`  | List of Neptune run IDs to fetch. |
| `custom_ids` | `Optional[List[str]]` | `None`  | List of custom run IDs to fetch.  |

__Returns:__ Iterator of ReadOnlyRun objects.

__Example:__

```python
project = ReadOnlyProject()

for run in project.fetch_read_only_runs(custom_ids=["nostalgic_shockley", "high_albattani"]):
    ...
```

---

#### `fetch_read_only_experiments()`

Lists experiments of the project in the form of ReadOnlyRun.

__Parameters:__

| Name    | Type                  | Default | Description                        |
|---------|-----------------------|---------|------------------------------------|
| `names` | `Optional[List[str]]` | `None`  | List of experiment names to fetch. |

__Returns:__ Iterator of ReadOnlyRun objects.

__Example:__

```python
project = ReadOnlyProject()

for run in project.fetch_read_only_experiments(names=["yolo-v2", "yolo-v3"]):
    ...
```

---

### `ReadOnlyRun`

Representation of a Neptune run in a limited read-only mode.

#### Initialization

Can be created

- with the class constructor:

    ```python
    project = ReadOnlyProject()
    run = ReadOnlyRun(project, with_id="TES-1")
    ```

- or as a result of the [`fetch_read_only_runs()`](#fetch_read_only_runs) method of the `ReadOnlyProject` class:

    ```python
    for run in project.fetch_read_only_runs(
        custom_ids=["nostalgic_shockley", "high_albattani"]):
        ...
    ```

__Parameters:__

| Name                | Type              | Default | Description                                                                                                                      |
|---------------------|-------------------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| `read_only_project` | `ReadOnlyProject` | -       | Project from which the run is fetched.                                                                                           |
| `with_id`           | `Optional[str]`   | `None`  | ID of the Neptune run to fetch. Example: `RUN-1`. Exclusive with the `custom_id` and `experiment_name` parameters.               |
| `custom_id`         | `Optional[str]`   | `None`  | Custom ID of the Neptune run to fetch. Example: `high_albattani`. Exclusive with the `with_id` and `experiment_name` parameters. |
| `experiment_name`   | `Optional[str]`   | `None`  | Name of the Neptune experiment to fetch. Example: `high_albattani`. Exclusive with the `with_id` and `custom_id` parameters.     |

__Example:__

```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

project = ReadOnlyProject("workspace-name/project-name", api_token="...")
run = ReadOnlyRun(project, custom_id="high_albattani")
```

---

#### `.field_names`

List of run field names.

A _field_ is the location where a piece of metadata is stored in the run.

__Returns:__ Iterator of run fields as strings.

__Example:__

```python
for run in project.fetch_read_only_runs(custom_ids=["nostalgic_shockley", ...]):
    print(list(run.field_names))
```

---

#### Field lookup: `run[field_name]`

Used to access a specific field of a run. See [Available types](#available-types).

__Returns:__ An internal object used to operate on a specific field.

__Example:__

```python
run = ReadOnlyRun(...)
custom_id = run["sys/custom_run_id"].fetch()
```

---

#### `prefetch()`

Pre-fetches a batch of fields to the internal cache.

Improves the performance of access to consecutive field values.

Supported Neptune field types:

- [`Boolean`](#boolean)
- [`Datetime`](#datetime)
- [`Float`](#float)
- [`FloatSeries`](#floatseries)
- [`Integer`](#integer)
- [`ObjectState`](#objectstate)
- [`String`](#string)
- [`StringSet`](#stringset)

__Parameters:__

| Name    | Type        | Default | Description                                |
|---------|-------------|---------|--------------------------------------------|
| `paths` | `List[str]` | -       | List of field paths to fetch to the cache. |

__Example:__

```python
run = ReadOnlyRun(...)
run.prefetch(["parameters/optimizer", "parameter/init_lr"])
# No more calls to the API
print(run["parameters/optimizer"].fetch())
print(run["parameter/init_lr"].fetch())
```

### `prefetch_series_values()`

Prefetches a batch of series to the internal cache.

Improves the performance of access to consecutive field values. Works only for series ([`FloatSeries`](#floatseries)).

To speed up the fetching process, this method can use multithreading.
To enable it, set the `use_threads` parameter to `True`.

By default, the maximum number of workers is 10. You can change this number by setting the `NEPTUNE_FETCHER_MAX_WORKERS`
environment variable.

__Parameters__:

| Name                | Type                  | Default      | Description                                                                                                                                                                                                                                                                                                                |
|---------------------|-----------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `paths`             | `List[str]`, required | `None`       | List of paths to prefetch to the internal cache.                                                                                                                                                                                                                                                                           |
| `use_threads`       | `bool`, optional      | `False`      | Whether to use threads to fetch the data.                                                                                                                                                                                                                                                                                  |
| `progress_bar`      | `ProgressBarType`     | `None`       | Set to False to disable the download progress bar, or pass a ProgressBarCallback class to use your own progress bar. If set to None or True, the default tqdm-based progress bar is used.                                                                                                                                  |
| `include_inherited` | `bool`, optional      | `True`       | If True (default), values inherited from ancestor runs are included. To only fetch values from the current run, set to False.                                                                                                                                                                                              |
| `step_range`        | `tuple[float, float]` | (None, None) | Limits the range of steps to fetch. This must be a 2-tuple: <br> - `left`: The left boundary of the range (exclusive). If `None`, the range extends indefinitely on the left.<br>- `right`: (currently not supported) The right boundary of the range (inclusive). If `None`, the range extends indefinitely on the right. |

__Example__:

```python
run.prefetch_series_values(["metrics/loss", "metrics/accuracy"])
# No more calls to the API
print(run["metrics/loss"].fetch_values())
print(run["metrics/accuracy"].fetch_values())
```

## Available types

This section lists the available field types and data retrieval operations.

---

### `Boolean`

#### `fetch()`

Retrieves a `bool` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
status = run["sys/failed"].fetch()
```

---

### `Datetime`

#### `fetch()`

Retrieves a `datetime.datetime` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
created_at = run["sys/creation_time"].fetch()
```

---

### `Float`

#### `fetch()`

Retrieves a `float` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
f1 = run["scores/f1"].fetch()
```

---

### `FloatSeries`

#### `fetch()` or `fetch_last()`

Retrieves the last value of a series, either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Returns:__ `Optional[float]`

__Example:__

```python
loss = run["loss"].fetch_last()
```

#### `fetch_values()`

Retrieves all series values either from the internal cache (see [`prefetch_series_values()`](#prefetch_series_values))
or from the API.

__Parameters:__

| Name                | Type                  | Default      | Description                                                                                                                                                                                         |
|---------------------|-----------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `include_timestamp` | `bool`                | `True`       | Whether the fetched data should include the timestamp field.                                                                                                                                        |
| `include_inherited` | `bool`                | `True`       | If True (default), values inherited from ancestor runs are included. To only fetch values from the current run, set to False.                                                                       |
| `progress_bar`      | `ProgressBarType`     | `None`       | Set to False to disable the download progress bar, or pass a ProgressBarCallback class to use your own progress bar. If set to None or True, the default tqdm-based progress bar is used.           |
| `step_range`        | `tuple[float, float]` | (None, None) | - left: left boundary of the range (exclusive). If None, it\'s open on the left. <br> - right: (currently not supported) right boundary of the range (inclusive). If None, it\'s open on the right. |

__Returns:__ `pandas.DataFrame`

__Example:__

```python
values = run["loss"].fetch_values()
```

---

### `Integer`

#### `fetch()`

Retrieves an `int` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
batch_size = run["batch_size"].fetch()
```

---

### `ObjectState`

#### `fetch()`

Retrieves the state of a run either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Returns:__ `str`

> [!NOTE]
> The state can be **active** or **inactive**. It refers to whether new data was recently logged to the run.
> To learn more about this field, see [System namespace: State](https://docs.neptune.ai/api/sys/#state) in the Neptune
> docs.

__Example:__

```python
state = run["sys/state"].fetch()
```

---

### `String`

#### `fetch()`

Retrieves a `str` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
token = run["token"].fetch()
```

---

### `StringSet`

#### `fetch()`

Retrieves a `dict` of `str` values either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
groups = run["sys/group_tags"].fetch()
```

## License

This project is licensed under the Apache License Version 2.0. For more details,
see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
