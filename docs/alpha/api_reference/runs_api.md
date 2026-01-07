# Working with runs

Experiments consist of runs. By default, the Fetcher API returns metadata only from experiment head runs, not all individual runs.

To target runs instead of experiments, you have two options:

## A) Import methods from `runs` module

Import one or more fetching methods from the `runs` module:

```py
from neptune_fetcher.alpha.runs import (
    fetch_metrics,
    fetch_runs_table,
    list_attributes,
    list_runs,
)


fetch_metrics(
    runs=...,
    ...
)
```

## B) Specify `runs` in each call

Import the `runs` module and prepend `.runs` to each fetching method call:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import runs


runs.fetch_metrics(
    runs=...,
    ...
)
```

In both cases, instead of the `experiments` parameter, use `runs`.

Note the difference in identifying runs versus experiments:

- Experiments are characterized by name (`sys/name`).
- Runs are characterized by ID (`sys/custom_run_id`).

Example:

```py
import neptune_fetcher.alpha as npt
from neptune_fetcher.alpha import runs


runs.fetch_metrics(
    runs=r"^speedy-seagull.*_02",  # run ID
    attributes=r"losses/.*",
)
```
