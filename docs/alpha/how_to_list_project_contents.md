# List project contents

To retrieve the names of runs, experiments, or attributes present in a project, use the `list_*()` methods of the Fetcher API.

## Listing experiments

To list experiments in a project, use `list_experiments()`.

To list only experiments whose name match a certain regex pattern:

```python
import neptune_fetcher.alpha as npt


npt.list_experiments(r"exp_.*")
```

```pycon title="Output"
['exp_xjjrq', 'exp_ymgun', 'exp_nmjbt', 'exp_ixgwm', 'exp_rdmuw']
```

## Listing runs

To list runs in a project, use `list_runs()`:

```py
from neptune_fetcher.alpha import runs


runs.list_runs(runs=r"kittiwake_02.*25$")
```

```pycon title="Output"
['onerous-kittiwake_0287625', 'spurious-kittiwake_025c425']
```

## Listing unique attributes

To list attributes present in a project, use `list_attributes()`.

The following example looks for attributes matching the regular expression `metrics.*|config/.*`, among experiments whose name match `exp.*`:

```python
npt.list_attributes(
    experiments=r"exp.*",
    attributes=r"metrics.*|config/.*",
)
```

```pycon title="Output"
['config/batch_size',
 'config/epochs',
 'config/last_event_time',
 'config/learning_rate',
 'config/optimizer',
 'config/use_bias',
 'metrics/accuracy',
 'metrics/loss',
 'metrics/val_accuracy',
 'metrics/val_loss']
```
