# `with_api_token()`

Copies the context and overwrites the `api_token` field with the provided Neptune API token.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_token` | str | — | Your API token or a service account's API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. |

## Example

Get the global context, change the Neptune API token, and use the new context:

```py
import neptune_fetcher.alpha as npt


other_account = npt.get_context().with_api_token("SomeOtherNeptuneApiToken")
npt.list_experiments(experiments=r"exp_.*", context=other_account)
```
