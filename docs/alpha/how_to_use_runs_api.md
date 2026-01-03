# Fetch run metadata

Experiments consist of runs. By default, the Fetcher API returns metadata only from experiment head runs, not all individual runs.

You can use the following fetching methods for runs:

- `download_files`
- `fetch_metrics()`
- `fetch_runs_table()`
- `fetch_series()`
- `list_attributes()`
- `list_runs()`

To work with runs in your code:

1. Import the `runs` module:

    ```py
    from neptune_fetcher.alpha import runs
    ```

1. Prepend `runs` to the method call:

    ```py
    runs.fetch_metrics()
    ```

1. Instead of the `experiments` parameter, use `runs`:

    ```py
    runs.fetch_metrics(
        runs=...
    )
    ```

Note the difference in identifying runs versus experiments:

- Experiments are characterized by name (`sys/name`).
- Runs are characterized by ID (`sys/custom_run_id`).

## Example

```py
from neptune_fetcher.alpha import runs


runs.fetch_metrics(
    runs=r"^speedy-seagull.*_02",  # run ID
    attributes=r"losses/.*",
)
```
