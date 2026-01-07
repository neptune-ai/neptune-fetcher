# Construct filters with Fetcher API

Use the `Filter` constructor to specify criteria when fetching experiments or runs.

> Examples of filters:
>
> - Name or attribute must match regular expression.
> - Attribute value must pass a condition, like "greater than 0.9".

You can negate a filter or join multiple filters with logical operators.

To use a filter in a query, pass it as the argument to a fetching or listing method:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.filters import Filter


npt.fetch_experiments_table(Filter.lt("validation/loss", 0.1))
```

## Filter experiments

### Specify experiments

The simplest filter is a string representing a regular expression. To pass the filter, the experiment name must match the regex:

```py
name_regex = r"week-4\d$"

npt.fetch_experiments_table(name_regex)
```

To specify exact experiments, pass a list of experiment names directly to the `experiments` argument:

```py
npt.fetch_experiments_table(
    experiments=["kittiwake_week-1", "kittiwake_week-2", "kittiwake_week-3"]
)
```

> [!TIP]
> To specify exact runs, experiments, or attributes, you can also use the `name_eq()` and `name_in()` methods.

### Filter on string value

Require the username of the account that created the run to match a regular expression:

```py
owner_filter = Filter.matches_all("sys/owner", r"^vidar")
```

Require a custom string field to equal a value:

```py
optimizer_filter = Filter.eq("config/optimizer", r"Adam")
```

### Filter on numerical value

Set a criterion for the last logged value of a metric:

```py
loss_filter = Filter.lt("validation/loss", 0.1)
```

> For more control over the selected metric, use the [Attribute](#specify-metric-aggregation) constructor.

Require a certain learning rate:

```py
lr_filter = Filter.eq("configs/learning_rate", 0.01)
```

### Filter on tags

Don't include experiments tagged with "test" or "val":

```py
no_test_or_val = Filter.contains_none("sys/tags", ["test", "val"])
```

## Filter runs

The `runs` module supports all of the filter classes.

To filter runs, pass the filter object to the `runs` argument:

```py
from neptune_fetcher.alpha import runs


tiny_loss = Filter.lt("validation/loss", 0.05)
runs.fetch_runs_table(runs=tiny_loss)
```

### Specify runs

To specify exact runs to fetch, pass a list of run IDs to the `runs` argument:

```py
npt.fetch_runs_table(
    runs=["spurious-kittiwake_025c425", "spurious-kittiwake_x56jjh2"]
)
```

## Specify attributes to return as columns

When fetching metadata with `fetch_experiments_table()`, in the returned table, each column represents an attribute.

To select specific metrics or other attributes based on various criteria, use the `AttributeFilter` constructor.

To use an attribute filter, pass it to the `attributes` argument of `fetch_experiments_table()`:

```py
npt.fetch_experiments_table(
    attributes=AttributeFilter(...),
)
```

### Examples

The simplest filter is a string representing a regular expression. Attribute names that match the regex `config(s)?/` are returned as columns:

```py
npt.fetch_experiments_table(attributes=r"config(s)?/")
```

To specify exact attributes to fetch, pass a list directly to the `attributes` argument:

```py
npt.fetch_experiments_table(attributes=["configs/optimizer", "configs/batch_size"])
```

Select metrics that don't match regexes `^test` or `loss$` and pick the "average" and "variance" aggregations:

```py
AttributeFilter(
    type_in=["float_series"],
    name_matches_none=[r"^test", r"loss$"],
    aggregations=["average", "variance"],
)
```

In this case, the returned table includes "average" and "variance" columns for each metric:

```pycon title="Output"
attribute              train/accuracy             validation/accuracy
aggregation                   average  variance               average  variance
experiment
exp-1738662528               0.000000  0.000273                   0.0  0.000269
exp-1738325381               0.000000  0.594614                   0.0  0.595119
...
```

## Specify metric aggregation

The `Attribute` constructor is used for specifying a single attribute and picking a metric aggregation function.

When fetching experiments or runs, use this class to filter and sort the returned entries.

For example, to pick the variance of the `val/loss` values:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.filters import Attribute


val_loss_variance = Attribute(
    name="val/loss",
    aggregation="variance",
)
```

Construct a filter around the attribute, then pass it to a fetching or listing method:

```py
tiny_val_loss_variance = Filter.lt(val_loss_variance, 0.01)
npt.fetch_experiments_table(experiments=tiny_val_loss_variance)
```

## Combine or negate filters

You can operate on all filter types and use them together.

To negate a filter, pass it to `negate()` or prepend it with `~`:

```py
from neptune_fetcher.alpha.filters import Filter


not_loss_filter = ~loss_filter
# equivalent to
not_loss_filter = Filter.negate(loss_filter)
```

To join filters:

- AND: Use `&` or pass the filters to the `all()` method.
- OR: Use `|` or pass the filters to the `any()` method.

```py title="Example"
name_and_loss_filter = name_filter & loss_filter
# equivalent to
name_and_loss_filter = Filter.all(name_filter, loss_filter)
```
