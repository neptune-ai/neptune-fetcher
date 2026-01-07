# `fetch_metrics()`

Returns raw values for the requested metrics. The returned values don't include any aggregation, approximation, or interpolation.

## Fetch experiment metrics

You can filter the results by:

- Experiments: Specify which experiments to search.
- Attributes: Only list attributes that match certain criteria.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| list[str] \| [Filter](filter.md) | — | A filter specifying which experiments to include: - a regex that the experiment name must match, or - a list of specific experiment names, or - a Filter object. |
| `attributes` | str \| list[str] \| AttributeFilter | — | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an [`AttributeFilter`](attributefilter.md) object. If `AttributeFilter.aggregations` is set, an exception will be raised as they're not supported in this function. |
| `include_time` | "absolute" | None | Whether to include absolute timestamp. If set, each metric column has an additional sub-column with requested timestamp values. |
| `step_range` | Tuple[float, float] | None | A tuple specifying the range of steps to include. Can represent an open interval. |
| `lineage_to_the_root` | bool | True | If `True`, includes all points from the complete experiment history. If `False`, only includes points from the selected experiment. |
| `tail_limit` | int | None | From the tail end of each series, how many points to include at most. |
| `type_suffix_in_column_names` | bool | False | If `True`, columns of the returned DataFrame will be suffixed with `":<type>"`. For example, `"attribute1:float_series"`, `"attribute1:string"`. If set to `False`, the method throws an exception if there are multiple types under one path. |
| `include_point_previews` | bool | False | If set to `True`, metric previews are included in the fetched data frame. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

### Example

Fetch loss metrics from experiments matching a regex, including point previews and only values logged from step 1000 onward:

```py
import neptune_fetcher.alpha as npt


npt.fetch_metrics(
    experiments=r"seagull.*_estimated$",
    attributes=r"^loss/.*",
    step_range=(1000.0, None),
    include_point_previews=True,
)
```

Sample output:

```pycon
                            path     loss/train
                                     is_preview     preview_completion	 value
               experiment   step
seagull-45xc099_estimated   1000.0        False	                   1.0	 0.193153
                            1001.0        False	                   1.0	 0.166237
                            1002.0        False	                   1.0	 0.12602
...
```

## Fetch run metrics

You can filter the results by:

- Runs: Specify which runs to search.
- Attributes: Only list attributes that match certain criteria.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `runs` | str \| list[str] \| [Filter](filter.md) | — | A filter specifying which runs to include: - a regex that the run ID must match, or - a list of specific run IDs, or - a Filter object. |
| `attributes` | str \| list[str] \| [AttributeFilter](attributefilter.md) | — | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an AttributeFilter object. If AttributeFilter.aggregations is set, an exception will be raised as they're not supported in this function. |
| `include_time` | "absolute" | None | Whether to include absolute timestamp. If set, each metric column has an additional sub-column with requested timestamp values. |
| `step_range` | Tuple[float, float] | None | A tuple specifying the range of steps to include. Can represent an open interval. |
| `lineage_to_the_root` | bool | True | If `True`, includes all points from the complete run history. If `False`, only includes points from the selected run. |
| `tail_limit` | int | None | From the tail end of each series, how many points to include at most. |
| `type_suffix_in_column_names` | bool | False | If `True`, columns of the returned DataFrame will be suffixed with `":<type>"`. For example, `"attribute1:float_series"`, `"attribute1:string"`. If set to `False`, the method throws an exception if there are multiple types under one path. |
| `include_point_previews` | bool | False | If set to `True`, metric previews are included in the fetched data frame. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

### Example

Fetch accuracy metrics from runs matching a regex, including the last 3 values from each series:

```py
from neptune_fetcher.alpha import runs


runs.fetch_metrics(
    runs=r"marigold",
    attributes=r"accuracy",
    tail_limit=3,
)
```

Sample output:

```pycon
                                    accuracy
                              run   step
arrogant-millipede+marigold-finch   49.0	0.830062
                                    50.0	0.828428
                                    51.0	0.825925
 marigold-finch+realistic-dolphin   31.0	0.970358
                                    32.0	0.986717
                                    33.0	0.971492
 marigold-finch+thundering-mantis   29.0	0.925642
                                    30.0	0.963742
                                    31.0	0.970358
```
