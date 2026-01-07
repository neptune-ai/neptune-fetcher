# Migrate from original Fetcher API to Query API

To use the new Query API instead of the original Fetcher API, you need to update your scripts.

The following sections show the code differences side by side.

> [!NOTE]
> This guide is about the first experimental `neptune-fetcher` API for Neptune `3.x`.
>
> To migrate from the newer `neptune_fetcher.alpha` to `neptune_query`, see _Migrate from Fetcher Alpha to Query API_.

## Installation

Original Fetcher API:

```sh
pip install neptune-fetcher
```

Query API:

```sh
pip install "neptune-query<2.0.0"
```

## Imports

Original Fetcher API:

```py
from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)
```

Query API:

```py
import neptune_query as nq

# optional filter imports
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
```

### Runs API

To work with runs instead of experiments, import the `runs` module:

```py
import neptune_query.runs as nq_runs
```

## Setting the project and API token

Original Fetcher API:

```py
project = ReadOnlyProject(
    project="team-alpha/project-x",
    api_token="h0dHBzOi8aHR0cHM6...Y2MifQ==",
)
```

Query API:

```py
any_fetching_method(
    project="team-alpha/project-x",
    ...
)
```

> [!NOTE]
> In the Query API, the context is set automatically from environment variables. You can use the above method to override the default project.

## Listing experiments or runs of a project

Original Fetcher API:

```py
for exp in project.list_experiments():
    print(exp)

for run in project.list_runs():
    print(run)
```

Query API:

```py
nq.list_experiments()
nq_runs.list_runs()
```

> [!TIP]
> In the Query API, you can also use `list_attributes()` for experiments or runs.

## Fetching metadata as table

You can use a filter object to set criteria for the experiments to search, or for the attributes to include as columns in the returned table.

Original Fetcher API:

```py
project.fetch_experiments_df()
```

Query API:

```py
nq.fetch_experiments_table()
```

### Match experiment names

Original Fetcher API:

```py
project.fetch_experiments_df(
    names_regex=r"exp.*"
)
```

Query API:

```py
nq.fetch_experiments_table(
    experiments=r"exp.*"
)
```

### Match run IDs

Original Fetcher API:

```py
project.fetch_experiments_df(
    custom_id_regex=r"run.*"
)
```

Query API:

```py
nq_runs.fetch_runs_table(
    runs=r"run.*"
)
```

### Filter by metadata value

Original Fetcher API:

```py
project.fetch_experiments_df(
    query="(last(`acc`:floatSeries) > 0.88)"
)
```

Query API:

```py
nq.fetch_experiments_table(
    experiments=Filter.gt("acc", 0.88)
)
```

### Specify columns to include

Original Fetcher API:

```py
project.fetch_experiments_df(
    columns_regex=r"acc"
)
```

Query API:

```py
nq.fetch_experiments_table(
    attributes=r"acc"
)
```

## Fetching metric values

With `alpha`, you can fetch metric values from multiple experiments or runs at once.

Original Fetcher API:

```py
# For each experiment or run
run = ReadOnlyRun(
    ReadOnlyProject(...),
    experiment_name="exp123",
)
run.prefetch(["loss"])
run["loss"].fetch_values()
```
Query API:

```py
nq.fetch_metrics(
    experiments=r"exp.*",
    attributes=r"loss",
)
```

## Fetching a config or single value

To fetch the `f1` attribute value from a particular experiment:

Original Fetcher API:

```py title="Old API"
run = ReadOnlyRun(
    ReadOnlyProject(...),
    experiment_name="exp123",
)
f1_value = run["f1"].fetch()
```

Query API:

```py
f1_value = nq.fetch_experiments_table(
    experiments=["exp-123"],
    attributes=["f1"],
).iat[0, 0]
```
