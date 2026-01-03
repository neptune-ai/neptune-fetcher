# neptune-fetcher environment variables

Environment variables related to neptune-fetcher Python package.

## `NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS`

Controls HTTPX timeouts. The timeout is in seconds and applies to individual networking operations, such as connect, read, and write.

The default duration is `60`.

## `NEPTUNE_FETCHER_MAX_WORKERS`

Controls the number of workers in the thread pool, when using the `use_threads` parameter of the `prefetch_series_values()` method.

The default number is `10`.
