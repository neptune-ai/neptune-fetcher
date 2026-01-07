# `fetch_series()`

Fetches raw `string_series` values from selected experiments.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| [Filter](filter.md) | — | A filter specifying which experiments to fetch from: - a list of specific experiment names, or - a regex that the experiment name must match, or - a Filter object. |
| `attributes` | str \| [AttributeFilter](attributefilter.md) | — | A filter specifying which attributes to fetch: - a list of specific attribute names, or - a regex that the attribute name must match, or - an AttributeFilter object. |
| `include_time` | "absolute" | None | Whether to include absolute timestamp. If set, each metric column has an additional sub-column with requested timestamp values. |
| `step_range` | Tuple[float, float] | None | A tuple specifying the range of steps to include. Can represent an open interval. |
| `lineage_to_the_root` | bool | True | If `True`, includes all points from the complete experiment history. If `False`, only includes points from the selected experiment. |
| `tail_limit` | int | None | From the tail end of each series, how many points to include at most. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Fetch from runs

To fetch string series attributes from runs instead of experiments:

- Import the `fetch_series()` function from the `runs` module
- Replace the `experiments` parameter with `runs`
- Pass run IDs instead of experiment names

```py
from neptune_fetcher.alpha import runs


runs.fetch_series(
    runs=r"^speedy-seagull.*_02",  # run ID
    attributes=r"messages/.*",
)
```
