# Using the old Fetcher API

> [!IMPORTANT]
> We've released a new API that replaces Fetcher. See [Migrate to Query API][fetcher-migration] in the Neptune docs.

In your Python code, create a [`ReadOnlyProject`][read-only-project] instance:

```python
from neptune_fetcher import ReadOnlyProject

my_project = ReadOnlyProject()
```

Now you have a Neptune project to operate on.

> If you don't set the Neptune environment variables, you can pass your credentials through arguments when creating a project or run object.

To fetch experiments in bulk, call a fetching method on the project:

```python
experiments_df = my_project.fetch_experiments_df(
    names_regex="tree/.*",
    columns=["sys/custom_run_id", "sys/modification_time"],
    query='(last(`accuracy`:floatSeries) > 0.88) AND (`learning_rate`:float < 0.01)',
)
```

To fetch metadata from an individual experiment or run, create and use a [`ReadOnlyRun`][read-only-run] object:

```python
from neptune_fetcher import ReadOnlyRun

run = ReadOnlyRun(
    read_only_project=my_project,
    experiment_name="seagull-flying-skills",
)

# Fetch value
print(run["parameters/optimizer"].fetch())

# Fecth last value of metric
print(run["metrics/loss"].fetch_last())

# Fetch all metric values, with optional pre-fetching to speed up subsequent access to the field
run.prefetch_series_values(["metrics/accuracy"])
print(run["metrics/accuracy"].fetch_values())
```

For details, see:

- [API reference](docs/old/api_reference.md)
- [NQL reference](docs/old/nql_reference.md)

## Examples

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


[read-only-project]: api_reference.md#readonlyproject
[read-only-run]: api_reference.md#readonlyrun

[fetcher-migration]: https://docs-beta.neptune.ai/fetcher_migration
[nql]: https://docs-beta.neptune.ai/nql
[old-fetch-runs]: https://docs-beta.neptune.ai/fetch_runs
[old-fetch-metadata]: https://docs-beta.neptune.ai/fetch_run_data
