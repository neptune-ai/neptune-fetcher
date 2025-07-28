# Neptune Query API

The `neptune_query` package is a read-only API for fetching metadata tracked with the [Neptune logging client][neptune-client-scale].

With the Query API, you can:

- List experiments, runs, and attributes of a project.
- Fetch experiment or run metadata as a data frame.
- Define filters to fetch experiments, runs, and attributes that meet certain criteria.

## Installation

```bash
pip install "neptune-query>=1.0.0,<2.0.0"
```

Set your Neptune API token and project name as environment variables:

```bash
export NEPTUNE_API_TOKEN="ApiTokenFromYourNeptuneProfile"
```

```bash
export NEPTUNE_PROJECT="workspace-name/project-name"
```

For help, see [Get started][setup] in the Neptune documentation.

> **Note:** You can also pass the project path to the `project` argument of any querying function.

## Usage

```python
import neptune_query as nq
```

Available functions:

- `fetch_experiments_table()` &ndash; runs as rows and attributes as columns.
- `fetch_metrics()` &ndash; series of float or int values, with steps as rows.
- `fetch_series()` &ndash; for series of strings or histograms.
- `list_attributes()` &ndash; all logged attributes of the target project's experiment runs.
- `list_experiments()` &ndash; names of experiments in the target project.
- `set_api_token()` &ndash; set the Neptune API token to use for the session.

> To use the corresponding methods for runs, import the Runs API:
>
> ```python
> import neptune_query.runs as nq_runs
> ```
>
> You can use these methods to target individual runs by ID instead of experiment runs by name.

### Example 1: Fetch metric values

To fetch values at each step, use `fetch_metrics()`.

- To filter experiments to return, use the `experiments` parameter.
- To specify attributes to include as columns, use the `attributes` parameter.

```python
nq.fetch_metrics(
    experiments=["exp_dczjz"],
    attributes=r"metrics/val_.+_estimated$",
)
```

```pycon
                  metrics/val_accuracy_estimated  metrics/val_loss_estimated
experiment  step
exp_dczjz    1.0                        0.432187                    0.823375
             2.0                        0.649685                    0.971732
             3.0                        0.760142                    0.154741
             4.0                        0.719508                    0.504652
```

### Example 2: Fetch metadata as one row per run

To fetch experiment metadata from your project, use the `fetch_experiments_table()` function:

```python
nq.fetch_experiments_table(
    experiments=r"^exp_",
    attributes=["metrics/train_accuracy", "metrics/train_loss", "learning_rate"],
)
```

```pycon
            metrics/train_accuracy   metrics/train_loss   learning_rate
experiment
exp_ergwq                 0.278149             0.336344            0.01
exp_qgguv                 0.160260             0.790268            0.02
exp_hstrj                 0.365521             0.459901            0.01
```

> For series attributes, the value of the last logged step is returned.

---

## License

This project is licensed under the Apache License Version 2.0. For details, see [Apache License Version 2.0][license].


[setup]: https://docs.neptune.ai/setup

[neptune-client-scale]: https://github.com/neptune-ai/neptune-client-scale

[license]: http://www.apache.org/licenses/LICENSE-2.0
