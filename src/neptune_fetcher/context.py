from dataclasses import dataclass
from typing import Optional

from . import env

__all__ = (
    "Context",
    "get_context",
    "set_context",
    "set_project",
    "set_api_token",
)


@dataclass
class Context:
    project: Optional[str]
    api_token: Optional[str]
    proxies: Optional[dict[str, str]] = None


_CONTEXT: Optional[Context] = None


def get_context() -> Context:
    """
    Returns the global context.

    The context is automatically set from the environment variables (if they exist) on first access,
    if it's not set manually.
    The following environment variables are used:
    - NEPTUNE_PROJECT
    - NEPTUNE_API_TOKEN
    """
    global _CONTEXT
    if _CONTEXT is None:
        _CONTEXT = Context(
            project=env.NEPTUNE_PROJECT.get(),
            api_token=env.NEPTUNE_API_TOKEN.get(),
        )
    return _CONTEXT


def set_context(context: Optional[Context] = None) -> Context:
    """
    The context is automatically set from the environment variables (if they exist) on the first access,
    but it's possible to override it with this function.
    The following environment variables are used:
    - NEPTUNE_PROJECT
    - NEPTUNE_API_TOKEN
    Returns the set context.
    """
    global _CONTEXT
    _CONTEXT = context
    return get_context()


def set_project(project: str) -> Context:
    """
    Set the project in the context.
    Returns the set context.
    """
    context = get_context()
    context.project = project
    return context


def set_api_token(api_token: str) -> Context:
    """
    Set the API token in the context.
    Returns the set context.
    """
    context = get_context()
    context.api_token = api_token
    return context


def set_proxies(proxies: Optional[dict[str, str]]) -> Context:
    """
    Set the proxies in the context.
    Returns the set context.
    """
    context = get_context()
    context.proxies = proxies
    return context
