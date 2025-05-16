# Neptune Fetcher

Neptune Fetcher is a read-only API for querying metadata logged with the [Neptune Python client][neptune-client-scale]. The separation makes it safer and more efficient to fetch data from Neptune.

With the Fetcher API, you can:

- List experiments, runs, and attributes of a project.
- Fetch experiment or run metadata as a data frame.
- Define filters to fetch experiments, runs, and attributes that meet certain criteria.

> [!NOTE]
> _For the Python API corresponding to [Neptune 2.x][legacy-app], see [neptune-client][neptune-client]._

## Documentation

- [Fetching how-to guides][fetcher-guide]
- [Fetcher API reference][fetcher-api-ref]
- [Update your code from old Fetcher to Alpha][fetcher-migration]

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

For help, see [Get started][setup] in the Neptune documentation.

> **Note:** To change the token or project, you can [set the context][set-context] directly in the code. This way, you can work with multiple projects at the same time.

## Usage

Import the `alpha` module:

```python
import neptune_fetcher.alpha as npt
```

To fetch experiment metadata from your project, use the [`fetch_experiments_table()`][fetch-exp-table] function.

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

To fetch values at each step, use [`fetch_metrics()`][fetch-metrics]:

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

You can define detailed criteria for which experiments to search or which attributes to return.

For instructions, see the [how-to guides][fetcher-guide] in the Neptune documentation:

- [Listing project contents][project-explo]
- [Fetching metadata][fetch-data]
- [Constructing filters][construct-filters]
- [Working with runs][runs-api]

---

## Old Fetcher API

For documentation related to the previous version of the Fetcher API, see the `docs/old/` directory:

- [docs/old/usage.md](docs/old/usage.md)
- [docs/old/api_reference.md](docs/old/api_reference.md)

To update your code to the new version, see [Migrate to Fetcher Alpha][fetcher-migration] in the Neptune documentation.

---

## License

This project is licensed under the Apache License Version 2.0. For details, see [Apache License Version 2.0][license].

[fetcher-api-ref]: https://docs.neptune.ai/fetcher/attribute
[fetch-exp-table]: https://docs.neptune.ai/fetcher/fetch_experiments_table
[fetch-metrics]: https://docs.neptune.ai/fetcher/fetch_metrics

[construct-filters]: https://docs.neptune.ai/construct_fetching_filters
[fetch-data]: https://docs.neptune.ai/fetch_metadata
[fetcher-guide]: https://docs.neptune.ai/query_metadata
[fetcher-migration]: https://docs.neptune.ai/fetcher_migration
[project-explo]: https://docs.neptune.ai/list_project_contents
[runs-api]: https://docs.neptune.ai/fetcher_runs_api
[set-context]: https://docs.neptune.ai/set_fetching_context
[setup]: https://docs.neptune.ai/setup

[legacy-app]: https://app.neptune.ai/
[neptune-client]: https://github.com/neptune-ai/neptune-client
[neptune-client-scale]: https://github.com/neptune-ai/neptune-client-scale

[license]: http://www.apache.org/licenses/LICENSE-2.0
