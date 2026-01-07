# `with_project()`

Copies the context and overwrites the `project` field with the provided project.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project` | str | — | Name of a project in the form `workspace-name/project-name`. |

## Example

```py
import neptune_fetcher.alpha as npt


my_secondary_project = npt.get_context().with_project("team-beta/project-y")
npt.list_experiments(experiments=r"exp_.*", context=my_secondary_project)
```
