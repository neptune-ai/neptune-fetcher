# `fetch_experiments_table()`

Fetches a project's experiment metadata as a table.

Returns a data frame similar to the runs table in the Neptune app.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| list[str] \| [Filter](filter.md) | None | A filter specifying which experiments to include in the table: - a regex that the experiment name must match, or - a list of specific experiment names, or - a Filter object. If no filter is specified, all experiments are returned. |
| `attributes` | str \| list[str] \| [AttributeFilter](attributefilter.md) | "sys/name" | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an AttributeFilter object. If no filter is specified, only the experiment name is included. |
| `sort_by` | str \| [Attribute](attribute.md) | "sys/creation_time" | An attribute name or an Attribute object specifying type and, optionally, aggregation. |
| `sort_direction` | "asc" \| "desc" | "desc" | Sorting direction of the column specified by the `sort_by` parameter. |
| `limit` | int | None | Maximum number of experiments to return. By default all experiments are returned. |
| `type_suffix_in_column_names` | bool | False | If `True`, columns of the returned DataFrame are suffixed with `:<type>`. For example, `"attribute1:float_series"`, `"attribute1:string"`. If set to `False`, the method throws an exception if there are multiple types under one path. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Specifying aggregations

If the fetched attributes include metric attributes of type `float_series`, only the last logged value is returned by default.

To get different metric aggregations, pass an attribute filter to the `attributes` argument. In this case, metric aggregates are returned as sub-columns of a metric column. In pandas terms, the columns have a MultiIndex with two levels: "attribute name" as the first level and "aggregation" as the second level.
> For a demonstration, see the [Examples](#including-metric-aggregates) section.

The available aggregation functions are:

- `average`
- `last`
- `max`
- `min`
- `variance`

For details, see [AttributeFilter](attributefilter.md).

## Examples

Limit experiments to those meeting the following critera:

- Description must contain `exploration` and `new`
- The optimizer must be `Adam`
- The last logged accuracy score must be higher than 0.8

```py
from neptune_fetcher.alpha.filters import Filter

experiments_filter = (
    Filter.contains_all("sys/description", [r"exploration", r"new"])
    & Filter.eq("params/optimizer", "Adam")
    & Filter.gt("accuracy", 0.8)
)
```

From the matched experiments, return attributes that match `params/` as columns:

```py
npt.fetch_experiments_table(
    experiments=experiments_filter,
    attributes=r"params/",
)
```

Sample output:

```pycon
attribute	            params/batch_size	params/lr	params/optimizer
aggregation
             experiment
  seabird-flying-skills	               64               	0.002	Adam
seabird-swimming-skills	               32	                0.001	Adam
```

### Including metric aggregates

```py
import neptune_fetcher.alpha as npt


npt.fetch_experiments_table(
    experiments=r"exp_\d+",
    attributes=r".*metric.*/val_.+",
)
```

Sample output:

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

Fetch the average and variance of two accuracy metrics:

```py
from neptune_fetcher.alpha.filters import AttributeFilter


acc_avg_and_var = AttributeFilter(
    name_eq=["train/accuracy", "validation/accuracy"]
    aggregations=["average", "variance"],
)

npt.fetch_experiments_table(
    experiments=r"exp-\d+",
    attributes=acc_avg_and_var,
)
```

Sample output:

```pycon
attribute              train/accuracy             validation/accuracy
aggregation                   average  variance               average  variance
experiment
exp-1738662528               0.689133  0.000273               0.56911  0.000269
exp-1738325381               0.756778  0.594614               0.45659  0.595119
...
```
