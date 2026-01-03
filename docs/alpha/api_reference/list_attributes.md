# `list_attributes()`

You can filter the results by:

- Experiment or run: Specify which experiments or runs to search.
- Attributes: Only list attributes that match certain criteria.

## List experiment attributes

Returns a list of unique attribute names in a project's experiments.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| list[str] \| [Filter](filter.md) | None | A filter specifying experiments to which the attributes belong: - a regex that the experiment name must match, or - a list of specific experiment names, or - a Filter object |
| `attributes` | str \| list[str] \| [AttributeFilter](attributefilter.md) | None | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an AttributeFilter object. If AttributeFilter.aggregations is set, an exception will be raised as they're not supported in this function. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

### Example

List all attributes:

```py
import neptune_fetcher.alpha as npt


npt.list_attributes()
```

```pycon title="Output"
['Notes',
 'accuracy',
 'loss',
 'parameters/batch_size',
 'parameters/learning_rate',
 'parameters/optimizer',
 'parameters/use_preprocessing',
 'sys/creation_time',
 ...]
```

Filter the listed attributes:

```py
npt.list_attributes(
    experiments=r"kittiwake$",
    attributes=r"^sys"
)
```

```pycon title="Output"
['sys/creation_time',
 'sys/custom_run_id',
 'sys/description',
 'sys/experiment/is_head',
 'sys/experiment/name',
 ...]
```

## List run attributes

Returns a list of unique attribute names in a project's runs.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `runs` | str \| list[str] \| [Filter](filter.md) | None | A filter specifying runs to which the attributes belong: - a regex that the run ID must match, or - a list of specific run IDs, or - a Filter object |
| `attributes` | str \| list[str] \| [AttributeFilter](attributefilter.md) | None | A filter specifying which attributes to include in the table: - a regex that the attribute name must match, or - a list of specific attribute names, or - an AttributeFilter object. If AttributeFilter.aggregations is set, an exception will be raised as they're not supported in this function. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

### Examples

List all run attributes:

```py
from neptune_fetcher.alpha import runs


runs.list_attributes()
```

```pycon title="Output"
['Notes',
 'accuracy',
 'loss',
 'parameters/batch_size',
 'parameters/learning_rate',
 'parameters/optimizer',
 'parameters/use_preprocessing',
 'sys/creation_time',
 ...]
```

Filter the listed attributes:

```py
runs.list_attributes(
    runs=r"kittiwake_02.*25",
    attributes=r"^sys",
)
```

Sample output:

```pycon
['sys/creation_time',
 'sys/custom_run_id',
 'sys/description',
 'sys/experiment/is_head',
 'sys/experiment/name',
 ...]
```
