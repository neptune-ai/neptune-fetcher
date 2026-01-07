# `get_context()`

Returns the globally set context.

## Examples

```pycon
>>> import neptune_fetcher.alpha as npt
>>> npt.get_context()
Context(project='team-alpha/project-x', api_token='eyJhcGlfXWRkcmVzcxI6Imh0dHBzOi...0In2=')
```

Copy the global context and use it with a different project path:

```py
my_secondary_project = npt.get_context().with_project("team-beta/project-y")

npt.list_experiments(experiments=r"exp_.*", context=my_secondary_project)
```
