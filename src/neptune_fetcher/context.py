from dataclasses import dataclass


@dataclass
class Context:
    project: str | None = None
    api_token: str | None = None


def set_context(context: Context | None = None) -> Context:
    """
    The context is automatically set from the environment variables (if they exist) on import of the module,
    but it's possible to override it with this function.
    The following environment variables are used:
    - NEPTUNE_PROJECT
    - NEPTUNE_API_TOKEN
    Returns the set context.
    """
    ...


def set_project(project: str) -> Context:
    """
    Set the project in the context.
    Returns the set context.
    """
    return set_context(Context(project=project))


def set_api_token(api_token: str) -> Context:
    """
    Set the API token in the context.
    Returns the set context.
    """
    return set_context(Context(api_token=api_token))