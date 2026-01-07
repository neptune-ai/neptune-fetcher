# `Attribute`

Helper for specifying an attribute and picking a metric aggregation function.

When fetching experiments or runs, use the `Attribute` constructor to filter and sort the returned entries.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | — | An attribute name to match exactly. |
| `aggregation` | "last" \| "min" \| "max" \| "average" \| "variance" | None | Aggregation function to apply when specifying a metric of type float_series. Defaults to `"last"`, that is, the last logged value. |
| `type` | "float" \| "int" \| "string" \| "bool" \| "datetime" \| "float_series" \| "string_set" \| "string_series" | None | Attribute type. Specify it to resolve ambiguity, in case some of the project's runs contain attributes that have the same name but are of a different type. |

## Examples

Select a metric and pick variance as the aggregation:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.filters import Attribute, Filter


val_loss_variance = Attribute(
    name="val/loss",
    aggregation="variance",
)
```

Construct a filter around the attribute with the [Filter](filter.md) class, then pass it to a fetching or listing method:

```py
tiny_val_loss_variance = Filter.lt(val_loss_variance, 0.01)
npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
```
