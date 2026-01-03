# `fetch_runs_table()`

Fetches a project's run metadata as a table.

Returns a data frame similar to the runs table in the Neptune app.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `runs` | str \| list[str] \| [Filter](filter.md) | None | A filter specifying which runs to include: - a regex that the run ID must match, or - a list of specific run IDs, or - a Filter object. If no filter is specified, all runs are returned. |
| `attributes` | str \| list[str] \| [AttributeFilter](attributefilter.md) | "sys/custom_run_id" | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an AttributeFilter object. If no filter is specified, the custom run ID and associated experiment name are included. |
| `sort_by` | str \| [Attribute](attribute.md) | "sys/creation_time" | An attribute name or an Attribute object specifying type and, optionally, aggregation. |
| `sort_direction` | "asc" \| "desc" | "desc" | Sorting direction of the column specified by the `sort_by` parameter. |
| `limit` | int | None | Maximum number of experiments to return. By default all experiments are returned. |
| `type_suffix_in_column_names` | bool | False | If `True`, columns of the returned DataFrame are suffixed with `:<type>`. For example, `"attribute1:float_series"`, `"attribute1:string"`. If set to `False`, the method throws an exception if there are multiple types under one path. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Specifying aggregations

If the fetched attributes include metric attributes of type `float_series`, only the last logged value is returned by default.

To get different metric aggregations, pass an attribute filter to the `attributes` argument. In this case, metric aggregates are returned as sub-columns of a metric column. In pandas terms, the columns have a MultiIndex with two levels: "attribute name" as the first level and "aggregation" as the second level.
> For a demonstration, see the [Examples](#examples) section.

The available aggregation functions are:

- `average`
- `last`
- `max`
- `min`
- `variance`

For details, see [AttributeFilter](attributefilter.md).

## Examples

```py
from neptune_fetcher.alpha import runs
from neptune_fetcher.alpha.filters import Filter


filtered_runs = (
    Filter.matches_none("sys/description", r"test")
    & Filter.le("params/lr", 0.002)
    & Filter.matches_all("sys/custom_run_id", r"trout")
)

runs.fetch_runs_table(
    runs=filtered_runs,
    attributes=r"params/",
    sort_by="params/lr",
)
```

Sample output:

```pycon
attribute       params/batch_size    params/lr    params/optimizer
aggregation
           run
trout-nk7la147                 64        0.002                Adam
trout-45hxc318                 64        0.001                Adam
trout-z6th1103                 64        0.001                Adam
```

### Including metric aggregates

```py
runs.fetch_runs_table(
    runs=r"kittiwake_02.*25",
    attributes=r".*metric.*/val_.+",
)
```

Sample output:

```pycon
                   metrics/val_accuracy metrics/val_loss
                                   last             last
run
kittiwake_0287625              0.278149         0.336344
kittiwake_025c425              0.160260         0.790268
kittiwake_02kj725              0.702490         0.366390
kittiwake_024vcx125            0.301545         0.917683
kittiwake_02gt11x25            0.999489         0.069839
```

Fetch the average and variance of two accuracy metrics:

```py
from neptune_fetcher.alpha.filters import AttributeFilter


acc_avg_and_var = AttributeFilter(
    name_eq=["train/accuracy", "validation/accuracy"]
    aggregations=["average", "variance"],
)

runs.fetch_runs_table(
    runs=r"kittiwake_02.*25",
    attributes=acc_avg_and_var,
)
```

Sample output:

```pycon
attribute              train/accuracy             validation/accuracy
aggregation                   average  variance               average  variance
run
kittiwake_0287625            0.689133  0.000273               0.56911  0.000269
kittiwake_025c425            0.756778  0.594614               0.45659  0.595119
...
```
