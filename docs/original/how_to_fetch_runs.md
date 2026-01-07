# Fetch runs or experiments

Similar to displaying and filtering runs in the runs table, you can fetch runs meeting certain criteria and choose which attributes to include as columns.

## Before you start

Install neptune-fetcher:

```py
pip install -U neptune-fetcher
```

## Step 1: Initialize read-only project

To create a read-only project to perform the fetching on, use:

```py
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject()
```

> [!TIP]
> If you haven't set your Neptune credentials as environment variables, you can pass the project name or API token as arguments:
>
> ```py
> project = ReadOnlyProject(
>     project="team-alpha/project-x",  # your full project name here
>     api_token="h0dHBzOi8aHR0cHM6...Y2MifQ==",  # your API token here
> )
> ```

## Step 2: Use fetching methods

Each fetching method has a variant for both experiments and runs:

- **Fetching experiments:** Only runs that represent current experiments are returned.

    > When fetching experiments that have a history of forked or restarted runs, the historical runs are not included.

- **Fetching runs:** All runs, including those that no longer represent experiments, are returned.

### Fetch metadata as data frame

To fetch an experiment's metadata as a pandas DataFrame, use `fetch_experiments_df()`:

```py
project = ReadOnlyProject()

all_experiments_df = project.fetch_experiments_df()
```

#### Filter by name or ID

You can use a regular expression to match experiment names:

```py title="Include experiments that match regex"
specific_experiments_df = project.fetch_experiments_df(
    names_regex=r"astute-.+-135"
)
```

```py title="Exclude experiments that match regex"
specific_experiments_df = project.fetch_experiments_df(
    names_exclude_regex=r"experiment-\d{2,4}"
)
```

> Neptune uses the RE2 regular expression library. For supported regex features and limitations, see the [RE2 syntax guide](https://github.com/google/re2/wiki/Syntax).

You can also fetch experiments by custom run ID:

```py
specific_experiments_df = project.fetch_experiments_df(
    custom_ids=["astute-kittiwake-14", "bombastic-seagull-2", "regal-xeme-18"]
)
```

```py
specific_experiments_df = project.fetch_experiments_df(
    custom_id_regex=r"[a-e]{2}_.+"
)
```

> The custom ID refers to the identifier set with the `run_id` argument at experiment creation.

#### Filter by metadata value

To construct a custom filter, use the `query` argument and the Neptune Query Language:

```py
experiments_df = project.fetch_experiments_df(
    query="(last(`accuracy`:floatSeries) > 0.88) AND (`f1`:float > 0.9)",
)
```

> [!NOTE]
> Single quotes aren't supported inside the `query` string. If wrapping values with quote marks, use double quotes:
>
> ```g4
> query='`sys/description`:string = "test on new data"'
> ```

#### Limit columns

To limit the number of returned columns, you can:

- specify columns with the `columns` argument
- retrieve extra columns that match a regular expression pattern with the `columns_regex` argument

For example:

```py
experiments_df = project.fetch_experiments_df(
    columns=["sys/modification_time", "scores/f1"],
    columns_regex=r"tree/.*",
)
```

#### Combine filters

If you combine multiple criteria, they're joined by the logical AND operator.

The below example returns experiments that meet the following criteria:

- The name matches the regular expression `tree/.*`
- The last logged `accuracy` value is higher than `0.9`
- The logged `learning_rate` value is less than `0.01`

Additionally, the returned data frame only includes the creation and modification times as columns.

```py
experiments_df = my_project.fetch_experiments_df(
    names_regex=r"tree/.*",
    query=r'(last(`accuracy`:floatSeries) > 0.9) AND (`learning_rate`:float < 0.01)',
    columns=["sys/creation_time", "sys/modification_time"],
)
```

### List project experiments or runs

To list the identifiers of all the experiments or runs of a project, use:

To list the identifiers of all the experiments of a project, use:

```py
project = ReadOnlyProject()

for experiment in project.list_experiments():
    print(experiment)
```

To list the identifiers of all the runs of a project, use:

```py
project = ReadOnlyProject()

for run in project.list_runs():
    print(run)
```

The above methods return the identifiers as an iterator of dictionaries.

To list the identifiers of all the experiments of a project as a data frame, use:

```py
project = ReadOnlyProject()

df = project.fetch_experiments()
```

To list the identifiers of all the runs of a project as a data frame, use:

```py
project = ReadOnlyProject()

df = project.fetch_runs()
```

### Fetch read-only experiments or runs

To download metadata from individual experiments or runs, fetch them as `ReadOnlyRun` objects.

For details, see [Fetch metadata from a run or experiment](how_to_fetch_run_data.md).
