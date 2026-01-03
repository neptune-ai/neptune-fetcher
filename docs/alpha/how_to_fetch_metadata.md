# Fetch metadata

To fetch a data frame similar to the table view in the web app, use the `fetch_*()` functions of the Fetcher API.

## Fetching metadata as table

To fetch experiment metadata from your project, use `fetch_experiments_table()`.
To limit the scope, use the filtering parameters:

- `experiments`: Only fetch attributes from experiments that pass the filter.
- `attributes`: Only include attributes that pass the filter as columns.

For both arguments, you can specify a simple string to match experiment or attribute names against. To construct more complex filters, use the `Filter` constructors.

```py
import neptune_fetcher.alpha as npt


npt.fetch_experiments_table(
    experiments=r"exp.*",
    attributes=r".*metric.*/val_.+",
)
```

```pycon title="Output"
           metrics/val_accuracy metrics/val_loss
                           last             last
experiment
exp_ergwq              0.278149         0.336344
exp_qgguv              0.160260         0.790268
exp_cnuwh              0.702490         0.366390
exp_kokxd              0.301545         0.917683
exp_gyvpp              0.999489         0.069839
```

> [!NOTE]
> Fetching metrics with `fetch_experiments_table()` or `fetch_runs_table()` returns an aggregated value for each attribute. The default aggregation is the last logged value.
>
> To fetch actual metric values at each step, see [Fetching metric values](#fetching-metric-values).

The following example fetches metadata from experiments that meet two criteria:

- The `validation/loss` attribute value is less than `0.1`
- The experiment name matches any in a specific list

Additionally, only attributes matching `validation` are included as columns.

```py
from neptune_fetcher.alpha.filters import Filter


loss_filter = Filter.lt("validation/loss", 0.1)
name_filter = Filter.name_in(["week-1", "week-2", "week-4", "week-7", "week-8"])

combined_filter = loss_filter & name_filter

npt.fetch_experiments_table(
    experiments=combined_filter,
    attributes=r"validation/",
)
```

```pycon title="Output"
            validation/accuracy   validation/loss    validation/f1
                           last              last             last
experiment
week-1                 0.278149          0.036344             0.56
week-2                 0.160260          0.090268             0.62
week-4                 0.702490          0.036390             0.77
week-7                 0.301545          0.091768             0.68
week-8                 0.999489          0.069839             0.81
```

## Fetching metric values

To fetch individual values from one or more <ApiElement is='float_series' /> attributes, use `fetch_metrics()`:

```py
npt.fetch_metrics(
    experiments=r"exp.*",
    attributes=r"metrics/.*",
)
```

```pycon title="Output"
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

### Fetch metric previews

To fetch point previews, set the `include_point_previews` argument to `True`:

```py
npt.fetch_metrics(
    experiments=r"exp.*",
    attributes=r"metrics/.*",
    include_point_previews=True,
)
```

When previews are included, the returned data frame includes additional sub-columns with preview information: `is_preview` and `preview_completion`.
