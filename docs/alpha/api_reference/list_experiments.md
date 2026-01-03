# `list_experiments()`

Returns a list of experiment names in a project. This function only targets the latest runs of experiments in the project.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| list[str] \| [Filter](filter.md) | None | A filter specifying which experiments to include: - a regex that the experiment name must match, or - a list of specific experiment names, or - a Filter object. |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Examples

List all experiments in a project:

```py
import neptune_fetcher.alpha as npt


npt.list_experiments()
```

Search a different project than the current global context and list only experiments whose name match a certain regex pattern:

```py
my_secondary_project = npt.get_context().with_project("team-beta/project-y")

npt.list_experiments(
    experiments=r"^seagull_.*_02$",
    context=my_secondary_project,
)
```

```pycon title="Output"
['seagull-dt35a_02', 'seagull-987kj_02', 'seagull-56hcs11_02']
```
