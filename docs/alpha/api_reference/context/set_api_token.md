# `set_api_token()`

Sets the Neptune API token in the context.

Returns the set context.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_token` | str | "" | Your API token or a service account's API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. |

## Example

```py
import neptune_fetcher.alpha as npt


npt.set_api_token("SomeOtherNeptuneApiToken")
```
