# `set_context()`

By default, the context is set from the `NEPTUNE_API_TOKEN` and `NEPTUNE_PROJECT` environment variables on import of the module. Use this function to override the default context.

If the argument is `None`, the global context is reset from environment variables.

Returns the set context.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `context` | [Context](context.md) | None | Neptune context, as defined by the project path and API token. |

## Example

```py
import neptune_fetcher.alpha as npt


my_secondary_project = Context(
    project="team-beta/project-y",
    api_token="SomeOtherNeptuneApiToken",
)
npt.set_context(my_secondary_project)
```
