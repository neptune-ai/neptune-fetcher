> [!IMPORTANT]
> This package is deprecated and no longer actively developed.
>
>
> Use [neptune-query](https://github.com/neptune-ai/neptune-query) instead:
>
> ```bash
> pip uninstall -y neptune-fetcher && pip install "neptune-query>=1.0.0,<2.0.0"
> ```
>
> For migration instructions, see:
> - [Migrate from Fetcher Alpha to Query API](docs/alpha/migrate_from_fetcher_alpha_to_query.md)
> - [Migrate from original Fetcher to Query API](docs/original/migrate_from_fetcher_to_query.md)

# Neptune Fetcher [DEPRECATED]

Neptune Fetcher is a read-only API for querying metadata.

With the Fetcher API, you can:

- List experiments, runs, and attributes of a project.
- Fetch experiment or run metadata as a data frame.
- Define filters to fetch experiments, runs, and attributes that meet certain criteria.

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

## Documentation

For how-tos and API references, see the [`docs/alpha`](docs/alpha/) directory.

> [!NOTE]
> For documentation related to the original Fetcher API based on `ReadOnlyProject` and `ReadOnlyRun`, see the [`docs/original`](docs/original/) directory.

## Usage

Import the `alpha` module:

```python
import neptune_fetcher.alpha as npt
```

To fetch experiment metadata from your project, use the `fetch_experiments_table()` function.

- To filter experiments to return, use the `experiments` parameter.
- To specify attributes to include as columns, use the `attributes` parameter.

```python
npt.fetch_experiments_table(
    experiments=["exp_ergwq", "exp_qgguv"],
    attributes=["metrics/train_accuracy", "metrics/train_loss"],
)
```

```pycon
           metrics/train_accuracy   metrics/train_loss
                             last                 last
experiment
exp_ergwq                0.278149             0.336344
exp_qgguv                0.160260             0.790268
```

To fetch values at each step, use `fetch_metrics()`:

```python
npt.fetch_metrics(
    experiments=r"exp.*",
    attributes=r".*metric.*/val_.+",
)
```

```pycon
  experiment  step  metrics/val_accuracy  metrics/val_loss
0  exp_dczjz     0              0.432187          0.823375
1  exp_dczjz     1              0.649685          0.971732
2  exp_dczjz     2              0.760142          0.154741
3  exp_dczjz     3              0.719508          0.504652
4  exp_dczjz     4              0.180321          0.503800
5  exp_hgctc     0              0.012390          0.171790
6  exp_hgctc     1              0.392041          0.768675
7  exp_hgctc     2                  None          0.847072
8  exp_hgctc     3              0.479945          0.323537
9  exp_hgctc     4              0.102646          0.055511
```

### Change the context

To change the token or project, use `Context`. It stores API token and project information. This way, you can work with multiple projects at the same time.

Example:

```py
import neptune_fetcher.alpha as npt


main_project = Context(project="team-alpha/project-x", api_token="SomeNeptuneApiToken")

# Use context for specific method call
npt.list_experiments(context=main_project)

# Set context globally
npt.set_context(main_project)

# Create a context by copying the global context and overriding the project
my_other_project = npt.get_context().with_project("team-beta/project-y")
# and pass it to any 'context' argument
```

---

## License

This project is licensed under the Apache License Version 2.0. For details, see [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
