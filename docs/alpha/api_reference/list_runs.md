# `list_runs()`

Returns a list of runs in a project. This function targets all runs in the project.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `runs` | str \| list[str] \| [Filter](filter.md) | — | A filter specifying which runs to include: - a regex that the run ID must match, or - a list of specific run IDs, or - a Filter object. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Examples

List all runs in a project:

```py
from neptune_fetcher.alpha import runs


runs.list_runs()
```

Search a different project than the current global context and list only runs whose ID match a certain regex pattern:

```py
my_secondary_project = npt.get_context().with_project("team-beta/project-y")

runs.list_runs(
    runs=r"kittiwake_02.*25$",
    context=my_secondary_project,
)
```

Sample output:

```pycon
['onerous-kittiwake_0287625', 'spurious-kittiwake_025c425']
```
