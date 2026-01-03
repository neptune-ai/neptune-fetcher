# `Filter`

Filter used to specify criteria when fetching experiments or runs.

## Methods for attributes

The following functions create a criterion based on the value of an attribute:

| Method            | Description                                                                        | Example |
| ----------------- | ---------------------------------------------------------------------------------- | ------- |
| `name_eq()`       | Run or experiment name equals the provided string.                                 | `name_eq("flying-123")` |
| `name_in()`       | Run or experiment name equals any of the provided strings.                         | `name_in("flying-123", "swimming-77")` |
| `eq()`            | Attribute value equals an int, float, str, or datetime value.                      | `eq("lr", 0.001)` |
| `ne()`            | Attribute value doesn't equal an int, float, str, or datetime value.               | `ne("sys/owner", "bot@my-workspace")` |
| `gt()`            | Attribute value is greater than an int, float, str, or datetime value.             | `gt("acc", 0.9)` |
| `ge()`            | Attribute value is greater than or equal to an int, float, str, or datetime value. | `ge("acc", 0.91)` |
| `lt()`            | Attribute value is less than an int, float, str, or datetime value.                | `lt("loss", 0.1)` |
| `le()`            | Attribute value is less than or equal to an int, float, str, or datetime value.    | `le("loss", 0.11)` |
| `matches_all()`   | <ApiElement is='string' /> attribute value matches a regex pattern or all in a list of regexes.     | `matches_all("optimizer", r"^Ada.*")` |
| `matches_none()`  | <ApiElement is='string' /> attribute value doesn't match a regex pattern or any in a of list of regexes. | `matches_none("optimizer", [r"momentum", r"RMS"])` |
| `contains_all()`  | <ul><li><ApiElement is='string_set' /> attribute contains a string or all in a list of strings.</li><li><ApiElement is='string' /> attribute value contains a substring or all in a list of substrings.</li></ul> | `contains_all("sys/tags", ["best", "v2.1"])` |
| `contains_none()` | <ul><li><ApiElement is='string_set' /> attribute doesn't contain a string or any in a list of strings.</li><li><ApiElement is='string' /> attribute value doesn't contain a substring or any in a list of substrings.</li></ul> | `contains_none("tokenizer", "bpe")` |
| `exists()`        | Attribute exists in the run or experiment.             | `exists("metric7")` |

## Methods for filters

The following methods take already defined Filter objects as arguments:

| Method            | Description                                                  | Example                 |
| ----------------- | ------------------------------------------------------------ | ----------------------- |
| `negate()`        | Negate a filter. Equivalent to prepending `~` to the filter. | `negate(Filter)`        |
| `all()`           | Concatenation (AND). Equivalent to joining filters with `&`. | `all(Filter1, Filter2)` |
| `any()`           | Alternation (OR). Equivalent to joining filters with `\|`.   | `any(Filter1, Filter2)` |

## Examples

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha.filters import Filter
```

Fetch experiments with a validation accuracy no higher than 0.6:

```py
low_val_acc = Filter.le("metrics/val/acc", 0.6)

npt.fetch_experiments_table(experiments=low_val_acc)
```

Combine multiple criteria:

```py
owned_by_me = Filter.eq("sys/owner", "vidar")
loss_filter = Filter.lt("validation/loss", 0.1)
tag_filter = Filter.contains_none("sys/tags", ["test", "buggy"])
dataset_check = Filter.exists("dataset_version")

my_interesting_experiments = owned_by_me & loss_filter & tag_filter & dataset_check

npt.fetch_experiments_table(experiments=my_interesting_experiments)
```

You can also pass an exact list directly to the `experiments`, `runs`, or `attributes` argument:

```py
npt.fetch_experiments_table(
    experiments=["daring-kittiwake_week-41", "discreet-kittiwake_week-42"]
)
```
