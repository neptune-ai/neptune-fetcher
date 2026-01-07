# `set_project()`

Sets the project in the context.

Returns the set context.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project` | str | — | Name of a project in the form `workspace-name/project-name`. |

## Example

```py
import neptune_fetcher.alpha as npt


npt.set_project("team-alpha/project-y")
```
