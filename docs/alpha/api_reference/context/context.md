# `Context`

To work with multiple projects simultaneously, use contexts. This way, you can set the scope for individual fetching calls or globally for your session.

## Parameters

| Parameter  | Type   | Default | Description |
|------------|--------|---------|-------------|
| `project`  | str    | None    | Name of a project in the form `workspace-name/project-name`. If `None`, the value of the `NEPTUNE_PROJECT` environment variable is used. |
| `api_token`| str    | None    | Your Neptune API token or a service account's API token. If `None`, the value of the `NEPTUNE_API_TOKEN` environment variable is used. |

## Methods

| Method               | Description |
|----------------------|-------------|
| `get_context()`      | Returns the currently set global context. |
| `set_context()`      | Sets a global context based on environment variables. |
| `set_api_token()`    | Sets a new API token globally. |
| `set_project()`      | Sets a new project globally. |
| `with_api_token()`   | Copies the context and overwrites the `api_token` field with the provided token. |
| `with_project()`     | Copies the context and overwrites the `project` field with the provided project. |

## Examples

```py
import neptune_fetcher.alpha as npt


main_project = Context(project="team-alpha/project-x", api_token="SomeNeptuneApiToken")
```

Use context for specific method call:

```py
npt.list_experiments(context=main_project)
```

Set context globally:

```py
npt.set_context(main_project)
```

Create a context by copying the global context and overriding the project:

```py
my_other_project = npt.get_context().with_project("team-beta/project-y")
# pass to any 'context' argument
```
