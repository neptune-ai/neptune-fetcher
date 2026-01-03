# `AttributeFilter`

Filter to apply to attributes when fetching runs or experiments.

Use to select specific metrics or other metadata based on various criteria.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name_eq` | str \| list[str] | None | An attribute name or list of names to match exactly. If `None`, this filter is not applied. |
| `type_in` | list[Literal["float", "int", "string", "bool", "datetime", "float_series", "string_set", "string_series"]] | all available types | A list of allowed attribute types. |
| `name_matches_all` | str \| list[str] | None | A regular expression or list of expressions that the attribute name must match. If `None`, this filter is not applied. |
| `name_matches_none` | str \| list[str] | None | A regular expression or list of expressions that the attribute names mustn't match. Attributes matching any of the regexes are excluded. If `None`, this filter is not applied. |
| `aggregations` | list[Literal["last", "min", "max", "average", "variance"]] | "last" | List of aggregation functions to apply when fetching metrics of type `float_series` or `string_series`. |

## Examples

Import the needed classes:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.filters import AttributeFilter
```

### Constructing a filter

Select metrics not matching regexes `^test` or `loss$` and pick the "average" and "variance" aggregations:

```py
AttributeFilter(
    name_matches_none=[r"^test", r"loss$"],
    aggregations=["average", "variance"],
)
```

Combine multiple filters with the pipe character:

```py
filter_1 = AttributeFilter(...)
filter_2 = AttributeFilter(...)
filter_3 = AttributeFilter(...)
alternatives = filter_1 | filter_2 | filter_3
```

### Using a filter

To use an attribute filter, pass it to the `attributes` argument of a fetching or listing method:

```py
npt.fetch_experiments_table(
    experiments=["daring-kittiwake_week-41", "discreet-kittiwake_week-42"],
    attributes=AttributeFilter(name_eq="configs/learning_rate"),
)
```

You can also pass a list of attribute names directly to the `attributes` argument. Example using the runs API:

```py
from neptune_fetcher.alpha import runs


runs.fetch_runs_table(
    runs=["spurious-kittiwake_025c425", "spurious-kittiwake_x56jjh2"],
    attributes=["configs/optimizer", "configs/batch_size"],
)
```
