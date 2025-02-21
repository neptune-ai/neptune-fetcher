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
pip install -U neptune-fetcher
```

Set your Neptune API token and project name as environment variables:

```bash
export NEPTUNE_API_TOKEN="h0dHBzOi8aHR0cHM.4kl0jvYh3Kb8...ifQ=="
```

```bash
export NEPTUNE_PROJECT="workspace-name/project-name"
```

For help, see https://docs-beta.neptune.ai/setup.

> **Note:** To change the token or project, you can [set the context](#working-with-multiple-projects) directly in the code. This way, you can work with multiple projects at the same time.

## Usage: Alpha version

To try the next major version of the Fetcher API, use the `alpha` module.

```python
import neptune_fetcher.alpha as npt
```

### Listing experiments

Filter experiments by name that match a regex:

```python
npt.list_experiments(r"exp_.*")
```

```pycon
['exp_xjjrq', 'exp_ymgun', 'exp_nmjbt', 'exp_ixgwm', 'exp_rdmuw']
```

### Listing unique attributes

Filter attributes by name and by experiment that they belong to:

```python
npt.list_attributes(
    experiments=r"exp.*",
    attributes=r"metrics.*|config/.*",
)
```

```pycon
['config/batch_size',
 'config/epochs',
 'config/last_event_time',
 'config/learning_rate',
 'config/optimizer',
 'config/use_bias',
 'metrics/accuracy',
 'metrics/loss',
 'metrics/val_accuracy',
 'metrics/val_loss']
```

### Fetching metadata table

To fetch experiment metadata from your project, use the `fetch_experiments_table()` function.

- To filter experiments to return, use the `experiments` parameter.
- To specify attributes to include as columns, use the `attributes` parameter.

For both arguments, you can specify a simple string to match experiment or attribute names against, or you can use the Filter classes to [construct more complex filters](#constructing-filters).

> Fetching metrics this way returns an aggregation value for each attribute. The default aggregation is the last logged value.
>
> To fetch actual metric values at each step, see [`fetch_metrics()`](#fetching-metric-values).

```python
npt.fetch_experiments_table(
    experiments=r"exp.*",
    attributes=r".*metric.*/val_.+",
)
```

```pycon
           metrics/val_accuracy metrics/val_loss
                           last             last
experiment
exp_ergwq              0.278149         0.336344
exp_qgguv              0.160260         0.790268
exp_cnuwh              0.702490         0.366390
exp_kokxd              0.301545         0.917683
exp_gyvpp              0.999489         0.069839
```

### Fetching metric values

To fetch individual values from one or more float series attributes, use the `fetch_metrics()` function:

```python
npt.fetch_metrics(
    experiments=r"exp.*",
    attributes=r"metrics/.*",
)
```

```pycon
  experiment  step  metrics/accuracy  metrics/loss  metrics/val_accuracy  metrics/val_loss
0  exp_dczjz     0          0.819754      0.588655              0.432187          0.823375
1  exp_dczjz     1          0.347907      0.297161              0.649685          0.971732
2  exp_dczjz     2          0.858863      0.988583              0.760142          0.154741
3  exp_dczjz     3          0.217097          None              0.719508          0.504652
4  exp_dczjz     4          0.089981      0.463146              0.180321          0.503800
5  exp_hgctc     0          0.318828      0.386347              0.012390          0.171790
6  exp_hgctc     1          0.035026      0.087053              0.392041          0.768675
7  exp_hgctc     2          0.004711      0.061848                  None          0.847072
8  exp_hgctc     3          0.359770      0.130022              0.479945          0.323537
9  exp_hgctc     4          0.007815      0.746344              0.102646          0.055511
```

#### Fetch metric previews

To fetch point previews, set the `include_point_previews` argument to `True`:

```python
npt.fetch_metrics(
    experiments=r"exp.*",
    attributes=r"metrics/.*",
    include_point_previews=True,
)
```

When previews are included, the returned data frame includes additional sub-columns with preview information (`is_preview` and `preview_completion`).

If `False`, only regular, committed points are fetched.

> For details, see [Metric previews][docs-metric-previews] in the documentation.

### Working with multiple projects

To work with multiple projects simultaneously, you can use contexts. This way, you can set the scope for individual fetching calls or globally for your session.

- To set a new project or API token globally, use `set_project()` or `set_api_token()`:

    ```python
    npt.set_project('some-workspace/another-project')
    ```

- To create a context object that you can pass to the `context` argument of a fetching method:

    ```python
    another_project_context = npt.get_context().with_project('some-workspace/another-project')
    npt.list_experiments(r"exp_.*", context=another_project_context)
    ```

Example flow:

1. `NEPTUNE_PROJECT` environment variable is read on module initialization:

    ```python
    import neptune_fetcher.alpha as npt
    ```

1. Work on the default project inferred from the environment variables:

    ```python
    npt.list_experiments(r"exp_.*")
    ```

    ```pycon
    ['exp_dhxxz', 'exp_saazg', 'exp_sxlrq', 'exp_vlqfe', 'exp_fpogj']
    ```

1. Work on another project without changing the global context:

    ```python
    another_project_ctx = npt.get_context().with_project('some-workspace/another-project')
    npt.list_experiments(r"exp_.*", context=another_project_ctx)
    ```

    ```pycon
    ['exp_oecez', 'exp_ytbda', 'exp_khfym']
    ```

1. Change the project globally:

    ```python
    npt.set_project('some-workspace/another-project')
    ```

1. Do some more work on another project:

    ```python
    npt.list_experiments(r"exp_.*")
    ```

    ```pycon
    ['exp_oecez', 'exp_ytbda', 'exp_khfym']
    ```

### Constructing filters

#### Filtering experiments

Use the Filter class to specify criteria when fetching experiments or runs.

> Examples of filters:
>
> - Name or attribute must match regular expression.
> - Attribute value must pass a condition, like "greater than 0.9".

You can negate a filter or join multiple filters with logical operators.

Methods available for attribute values:
- `eq()`: Equals
- `ne()`: Doesn't equal
- `gt()`: Greater than
- `ge()`: Greater than or equal to
- `lt()`: Less than
- `le()`: Less than or equal to
- `matches_all()`: Matches regex or all in list of regexes
- `matches_none()`: Doesn't match regex or any of list of regexes
- `contains_all()`: Tagset contains all tags, or string contains substrings
- `contains_none()`: Tagset doesn't contain any of the tags, or string doesn't contain the substrings
- `exists()`: Attribute exists

##### Examples

Import the needed classes:

```python
import neptune_fetcher.alpha as npt
from npt.filters import Attribute, Filter
```

Constructing filters:

A) Regex that the experiment or run name must match:

```python
name_filter = Filter.matches_all("sys/name", r"kittiwake$")
```

B) Don't allow the tags "test" or "val":

```python
no_test_or_val = Filter.contains_none("sys/tags", ["test", "val"])
```

C) Set a condition for the last logged value of a metric:

```python
loss_filter = Filter.lt("validation/loss", 0.1)
```

For more control over the selected metric, use the `Attribute()` helper class.

D) Negate a filter: Call `negate()` or prepend with `~`:

```python
not_loss_filter = ~loss_filter
# equivalent to
not_loss_filter = Filter.negate(loss_filter)
```

E) Combining filters:

- To join with AND: Use `&` or pass the filters to the `all()` method.
- To join with OR: Use `|` or pass the filters to the `any()` method.

```python
name_and_loss_filter = name_filter & loss_filter
# equivalent to
name_and_loss_filter = Filter.all(name_filter, loss_filter)
```

To use a filter in a query, pass it as the argument to a fetching or listing method:

```python
npt.fetch_experiments_table(experiments=name_and_loss_filter)
```

#### Filtering attributes

When fetching metadata with the `fetch_experiments_table()` function, in the returned table, each column represents an attribute.

To select specific metrics or other attributes based on various criteria, use the `AttributeFilter` class.

##### Examples

Select attribute by exact name:

```python
AttributeFilter(name_eq="config/optimizer")
```

Select metrics that don't match regexes `^test` or `loss$` and pick the "average" and "variance" aggregations:

```python
AttributeFilter(
    type_in=["float_series"],
    name_matches_none=[r"^test", r"loss$"],
    aggregations=["average", "variance"],
)
```

In this case, the returned table includes "average" and "variance" columns for each metric:

```pycon
attribute              train/accuracy             validation/accuracy
aggregation                   average  variance               average  variance
experiment
exp-1738662528               0.000000  0.000273                   0.0  0.000269
exp-1738325381               0.000000  0.594614                   0.0  0.595119
...
```

> For the types reference, see: https://docs-beta.neptune.ai/attribute_types

Combine multiple filters with the pipe character:

```python
filter_1 = AttributeFilter(...)
filter_2 = AttributeFilter(...)
filter_3 = AttributeFilter(...)
alternatives = filter_1 | filter_2 | filter_3
```

To use a filter, pass it to the `attributes` argument of the `fetch_experiments_table()` function:

```python
npt.fetch_experiments_table(
    experiments=...,
    attributes=AttributeFilter(...),
)
```

#### `Attribute` helper class

Used for specifying a single attribute and picking a metric aggregation function.

When fetching experiments or runs, use this class to filter and sort the returned entries.

##### Examples

Select a metric and pick variance as the aggregation:

```python
import neptune_fetcher.alpha as npt
from npt.filters import Attribute, Filter


val_loss_variance = Attribute(
    name="val/loss",
    aggregation="variance",
)
```

Construct a filter around the attribute, then pass it to a fetching or listing method:

```python
tiny_val_loss_variance = Filter.lt(val_loss_variance, 0.01)
npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
```

### Working with runs

Experiments consist of runs. By default, the Fetcher API returns metadata only from experiment head runs, not all individual runs.

To work with runs, you can use the corresponding run methods:

```python
from neptune_fetcher.alpha.runs import (
    fetch_metrics,
    fetch_runs_table,
    list_attributes,
    list_runs,
)
```

The usage is the same, except for the first parameter: For the run method calls, replace the `experiments` parameter with `runs`.

Note the difference in identifying runs versus experiments:

- Experiments are characterized by name (`sys/name`). This is the string passed to the `name` argument at creation.
- Runs are characterized by ID (`sys/custom_run_id`). This is the string passed to the `run_id` argument at creation.

Example:

```py
import neptune_fetcher.alpha as npt

npt.runs.fetch_metrics(
    runs=r"^speedy-seagull.*_02",  # run ID
    attributes=r"losses/.*",
)
```

---

## Usage: Fetcher `0.x`

> [!NOTE]
> We're redesigning the Fetcher API.
>
> To try the new version, see [Usage: Alpha version](#usage-alpha-version).

In your Python code, create a [`ReadOnlyProject`](#readonlyproject) instance:

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

To fetch metadata from an individual experiment or run, create and use a [`ReadOnlyRun`](#readonlyrun) object:

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

For details, see the Neptune documentation:

- [Fetch runs or experiments](https://docs-beta.neptune.ai/fetch_runs)
- [Fetch metadata from a run or experiment](https://docs-beta.neptune.ai/fetch_run_data)
- [Neptune Query Language (NQL)](https://docs-beta.neptune.ai/nql)


### Examples

#### Listing runs of a project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for run in project.list_runs():
    print(run)  # dicts with identifiers
```

#### Listing experiments of a project

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for experiment in project.list_experiments():
    print(experiment)  # dicts with identifiers
```

#### Fetching runs data frame with specific columns

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

runs_df = project.fetch_runs_df(
    columns=["sys/custom_run_id", "sys/modification_time"],
    columns_regex="tree/.*",  # added to columns specified with the "columns" parameter
)
```

#### Fetching data from specified runs

```python
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

for run in project.fetch_read_only_runs(with_ids=["RUN-1", "RUN-2"]):
    run.prefetch(["parameters/optimizer", "parameters/init_lr"])

    print(run["parameters/optimizer"].fetch())
    print(run["parameters/init_lr"].fetch())
```

#### Fetching data from a single run

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

### API reference

#### Supported regular expressions

Neptune uses the [RE2](https://github.com/google/re2) regular expression library. For supported regex features and limitations, see the official [syntax guide](https://github.com/google/re2/wiki/syntax).


#### `ReadOnlyProject`

Representation of a Neptune project in a limited read-only mode.

##### Initialization

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

##### `list_runs()`

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

##### `list_experiments()`

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

##### `fetch_runs()`

Fetches a table containing Neptune IDs, custom run IDs and names of runs in the project.

__Returns:__ `pandas.DataFrame` `pandas.DataFrame` with three columns (`sys/id`, `sys/name` and `sys/custom_run_id`)
and one row for each run.

__Example:__

```python
project = ReadOnlyProject()
df = project.fetch_runs()
```

---

##### `fetch_experiments()`

Fetches a table containing Neptune IDs, custom IDs and names of experiments in the project.

__Example__:

```python
df = project.fetch_experiments()
```

__Returns__:
`pandas.DataFrame` with three columns (`sys/id`, `sys/custom_run_id`, `sys/name`) and one row for each experiment.

---

##### `fetch_runs_df()`

Fetches the runs' metadata and returns them as a pandas DataFrame.

__Parameters:__

| Name              | Type                  | Default             | Description                                                                                                                                                                                                                                                                                                          |
|-------------------|-----------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `columns`         | `List[str]`, optional | `None`              | Names of columns to include in the table, as a list of field names. The sorting column, custom run identifier (`sys/custom_run_id`), and experiment name (`sys/name`) are always included. `None` results in returning only the default columns.
| `columns_regex`   | `str`, optional       | `None`              | A regex pattern to filter columns by name. Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
| `names_regex`     | `str`, optional       | `None`              | A regex pattern to filter the runs by name.                                                                                                                                                                                                                                                                          |
| `custom_id_regex` | `str`, optional       | `None`              | A regex pattern to filter the runs by custom ID.                                                                                                                                                                                                                                                                     |
| `with_ids`        | `List[str]`, optional | `None`              | List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                   |
| `custom_ids`      | `List[str]`, optional | `None`              | List of multiple custom IDs. Example: `["nostalgic_shockley", "high_albattani"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                              |
| `states`          | `List[str]`, optional | `None`              | List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the run. Matching any element of the list is sufficient to pass the criterion.                                                                                                                   |
| `owners`          | `List[str]`, optional | `None`              | List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the run. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                          |
| `tags`            | `List[str]`, optional | `None`              | A list of tags. Example: `"lightGBM"` or `["pytorch", "cycleLR"]`. **Note:** Only runs that have all specified tags will pass this criterion.                                                                                                                                                                        |
| `trashed`         | `bool`, optional      | `False`             | Whether to retrieve trashed runs. If `True`, only trashed runs are retrieved. If `False`, only non-trashed runs are retrieved. If `None` or left empty, all run objects are retrieved, including trashed ones.                                                                                                       |
| `limit`           | `int`, optional       | `None`              | Maximum number of runs to fetch. If `None`, all runs are fetched.                                                                                                                                                                                                                                                    |
| `sort_by`         | `str`, optional       | `sys/creation_time` | Name of the field to sort the results by. The field must represent a simple type (string, float, integer).                                                                                                                                                                                                           |
| `ascending`       | `bool`, optional      | `False`             | Whether to sort the entries in ascending order of the sorting column values.                                                                                                                                                                                                                                         |
| `progress_bar`    | `bool`                | `True`              | Set to `False `to disable the download progress bar.                                                                                                                                                                                                                                                                 |
| `query`           | `str`, optional       | `None`              | NQL query string. Example: `"(accuracy: float > 0.88) AND (loss: float < 0.2)"`. The query is applied on top of other criteria like, `custom_ids`, `tags` etc, using the logical AND operator. See examples below. For syntax, see [Neptune Query Language](https://docs.neptune.ai/usage/nql/) in the Neptune docs. |

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

Fetch runs with a complex query using NQL.

```python
runs_df = my_project.fetch_runs_df(
    query='(last(`accuracy`:floatSeries) > 0.88) AND (`learning_rate`:float < 0.01)'
)
```

---

##### `fetch_experiments_df()`

Fetches the experiments' metadata and returns them as a pandas DataFrame.

__Parameters__:

| Name                  | Type                  | Default             | Description                                                                                                                                                                                                                                                                                                          |
|-----------------------|-----------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `columns`             | `List[str]`, optional | `None`              | Names of columns to include in the table, as a list of field names. The sorting column, custom run identifier (`sys/custom_run_id`), and experiment name (`sys/name`) are always included. `None` results in returning only the default columns.
| `columns_regex`       | `str`, optional       | `None`              | A regex pattern to filter columns by name. Use this parameter to include columns in addition to the ones specified by the `columns` parameter.
| `names_regex`         | `str`, optional       | `None`              | A regex pattern to filter the experiments by name.                                                                                                                                                                                                                                                                   |
| `names_regex`         | `str`, optional       | `None`              | A regex pattern or a list of regex patterns to filter the experiments by name. Multiple patterns will be connected by AND logic.                                                                                                                                                                                     |
| `names_exclude_regex` | `str`, optional       | `None`              | A regex pattern or a list of regex patterns to exclude experiments by name. Multiple patterns will be connected by AND logic.                                                                                                                                                                                        |
| `custom_id_regex`     | `str`, optional       | `None`              | A regex pattern to filter the experiments by custom ID.                                                                                                                                                                                                                                                              |
| `with_ids`            | `List[str]`, optional | `None`              | List of multiple Neptune IDs. Example: `["NLU-1", "NLU-2"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                                                   |
| `custom_ids`          | `List[str]`, optional | `None`              | List of multiple custom IDs. Example: `["nostalgic_shockley", "high_albattani"]`. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                                              |
| `states`              | `List[str]`, optional | `None`              | List of states. Possible values: `"inactive"`, `"active"`. "Active" means that at least one process is connected to the experiment. Matching any element of the list is sufficient to pass the criterion.                                                                                                            |
| `owners`              | `List[str]`, optional | `None`              | List of multiple owners. Example:  `["frederic", "josh"]`. The owner is the user who created the experiement. Matching any element of the list is sufficient to pass the criterion.                                                                                                                                  |
| `tags`                | `List[str]`, optional | `None`              | A list of tags. Example: `"lightGBM"` or `["pytorch", "cycleLR"]`. **Note:** Only experiments that have all specified tags will pass this criterion.                                                                                                                                                                 |
| `trashed`             | `bool`, optional      | `False`             | Whether to retrieve trashed experiments. If `True`, only trashed experiments are retrieved. If `False`, only non-trashed experiments are retrieved. If `None` or left empty, all experiment objects are retrieved, including trashed ones.                                                                           |
| `limit`               | `int`, optional       | `None`              | Maximum number of experiments to fetch. If `None`, all experiments are fetched.                                                                                                                                                                                                                                      |
| `sort_by`             | `str`, optional       | `sys/creation_time` | Name of the field to sort the results by. The field must represent a simple type (string, float, integer).                                                                                                                                                                                                           |
| `ascending`           | `bool`, optional      | `False`             | Whether to sort the entries in ascending order of the sorting column values.                                                                                                                                                                                                                                         |
| `progress_bar`        | `bool`                | `True`              | Set to `False `to disable the download progress bar.                                                                                                                                                                                                                                                                 |
| `query`               | `str`, optional       | `None`              | NQL query string. Example: `"(accuracy: float > 0.88) AND (loss: float < 0.2)"`. The query is applied on top of other criteria like, `custom_ids`, `tags` etc, using the logical AND operator. See examples below. For syntax, see [Neptune Query Language](https://docs.neptune.ai/usage/nql/) in the Neptune docs. |

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

Use the Neptune Query Language to fetch experiments with a complex query. Note that for regular strings, the `\` character needs to be escaped:

```python
experiments_df = my_project.fetch_experiments_df(
    query='(`learning_rate`:float < 0.01) AND (`sys/name`:string MATCHES "experiment-\\\\d+")'
)
```

As a less cluttered alternative, pass a raw Python string to the `query` argument:

```python
experiments_df = my_project.fetch_experiments_df(
    query=r'(`learning_rate`:float < 0.01) AND (`sys/name`:string MATCHES "experiment-\\d+")'
)
```

---

##### `fetch_read_only_runs()`

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

##### `fetch_read_only_experiments()`

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

#### `ReadOnlyRun`

Representation of a Neptune run in a limited read-only mode.

##### Initialization

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

| Name                               | Type              | Default | Description                                                                                                                        |
|------------------------------------|-------------------|---------|------------------------------------------------------------------------------------------------------------------------------------|
| `read_only_project`                | `ReadOnlyProject` | -       | Project from which the run is fetched.                                                                                             |
| `with_id`                          | `Optional[str]`   | `None`  | ID of the Neptune run to fetch. Example: `RUN-1`. Exclusive with the `custom_id` and `experiment_name` parameters.                 |
| `custom_id`                        | `Optional[str]`   | `None`  | Custom ID of the Neptune run to fetch. Example: `high_albattani`. Exclusive with the `with_id` and `experiment_name` parameters.   |
| `experiment_name`                  | `Optional[str]`   | `None`  | Name of the Neptune experiment to fetch. Example: `high_albattani`. Exclusive with the `with_id` and `custom_id` parameters.       |
| `eager_load_fields` | `Optional[bool]`  | `True`  | Whether to eagerly load the run fields definitions. If `False`, individual fields are loaded only when accessed. Default is `True`. |

__Example:__

```python
from neptune_fetcher import ReadOnlyProject, ReadOnlyRun

project = ReadOnlyProject("workspace-name/project-name", api_token="...")
run = ReadOnlyRun(project, custom_id="high_albattani")
```

---

##### `.field_names`

List of run field names.

A _field_ is the location where a piece of metadata is stored in the run.

__Returns:__ Iterator of run fields as strings.

__Example:__

```python
for run in project.fetch_read_only_runs(custom_ids=["nostalgic_shockley", ...]):
    print(list(run.field_names))
```

---

##### Field lookup: `run[field_name]`

Used to access a specific field of a run. See [Available types](#available-types).

__Returns:__ An internal object used to operate on a specific field.

__Example:__

```python
run = ReadOnlyRun(...)
custom_id = run["sys/custom_run_id"].fetch()
```

---

##### `prefetch()`

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

#### `prefetch_series_values()`

Prefetches a batch of series to the internal cache. This method skips the non-existing attributes.

Improves the performance of access to consecutive field values. Works only for series ([`FloatSeries`](#floatseries)).

To speed up the fetching process, this method can use multithreading.
To enable it, set the `use_threads` parameter to `True`.

By default, the maximum number of workers is 10. You can change this number by setting the `NEPTUNE_FETCHER_MAX_WORKERS`
environment variable.

__Parameters__:

| Name                | Type                  | Default      | Description                                                                                                                                                                                                                                                                                      |
|---------------------|-----------------------|--------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `paths`             | `List[str]`, required | `None`       | List of paths to prefetch to the internal cache.                                                                                                                                                                                                                                                 |
| `use_threads`       | `bool`, optional      | `False`      | Whether to use threads to fetch the data.                                                                                                                                                                                                                                                        |
| `progress_bar`      | `bool`                | `True`       | Set to False to disable the download progress bar.                                                                                                                                                                                                                                               |
| `include_inherited` | `bool`, optional      | `True`       | If True (default), values inherited from ancestor runs are included. To only fetch values from the current run, set to False.                                                                                                                                                                    |
| `step_range`        | `tuple[float, float]` | (None, None) | Limits the range of steps to fetch. This must be a 2-tuple: <br> - `left`: The left boundary of the range (inclusive). If `None`, the range extends indefinitely on the left.<br>- `right`: The right boundary of the range (inclusive). If `None`, the range extends indefinitely on the right. |

__Example__:

```python
run.prefetch_series_values(["metrics/loss", "metrics/accuracy"])
# No more calls to the API
print(run["metrics/loss"].fetch_values())
print(run["metrics/accuracy"].fetch_values())
```

### Available types

This section lists the available field types and data retrieval operations.

---

#### `Boolean`

##### `fetch()`

Retrieves a `bool` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
status = run["sys/failed"].fetch()
```

---

#### `Datetime`

##### `fetch()`

Retrieves a `datetime.datetime` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
created_at = run["sys/creation_time"].fetch()
```

---

#### `Float`

##### `fetch()`

Retrieves a `float` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
f1 = run["scores/f1"].fetch()
```

---

#### `FloatSeries`

##### `fetch()` or `fetch_last()`

Retrieves the last value of a series, either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Returns:__ `Optional[float]`

__Example:__

```python
loss = run["loss"].fetch_last()
```

##### `fetch_values()`

Retrieves all series values either from the internal cache (see [`prefetch_series_values()`](#prefetch_series_values))
or from the API.

__Parameters:__

| Name                | Type                  | Default      | Description                                                                                                                                                               |
|---------------------|-----------------------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `include_timestamp` | `bool`                | `True`       | Whether the fetched data should include the timestamp field.                                                                                                              |
| `include_inherited` | `bool`                | `True`       | If True (default), values inherited from ancestor runs are included. To only fetch values from the current run, set to False.                                             |
| `progress_bar`      | `bool`                | `True`       | Set to False to disable the download progress bar.                                                                                                                        |
| `step_range`        | `tuple[float, float]` | (None, None) | - left: left boundary of the range (inclusive). If None, it\'s open on the left. <br> - right: right boundary of the range (inclusive). If None, it\'s open on the right. |
| `include_point_previews` | `bool` | `False` | To include [metric previews][docs-metric-previews], set to `True`. When previews are included, the returned data frame includes additional sub-columns with preview information (`is_preview` and `preview_completion`). If `False`, only regular, committed points are fetched. |

__Returns:__ `pandas.DataFrame`

__Example:__

```python
values = run["loss"].fetch_values()
```

---

#### `Integer`

##### `fetch()`

Retrieves an `int` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
batch_size = run["batch_size"].fetch()
```

---

#### `ObjectState`

##### `fetch()`

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

#### `String`

##### `fetch()`

Retrieves a `str` value either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
token = run["token"].fetch()
```

---

#### `StringSet`

##### `fetch()`

Retrieves a `dict` of `str` values either from the internal cache (see [`prefetch()`](#prefetch)) or from the API.

__Example:__

```python
groups = run["sys/group_tags"].fetch()
```

---

## License

This project is licensed under the Apache License Version 2.0. For more details,
see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).


[docs-metric-previews]: https://docs-beta.neptune.ai/metric_previews
