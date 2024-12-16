from dataclasses import dataclass
from typing import Optional

from . import env

__all__ = (
    "Context",
    "get_context",
    "set_context",
)


@dataclass
class Context:
    project: str
    api_token: str
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


def set_context(
    project: Optional[str] = None,
    api_token: Optional[str] = None,
    proxies: Optional[dict[str, str]] = None
) -> Context:
    """
    The context is automatically set from the environment variables (if they exist) on the first access,
    but it's possible to override it with this function.
    The following environment variables are used:
    - NEPTUNE_PROJECT
    - NEPTUNE_API_TOKEN
    """
    global _CONTEXT
    _CONTEXT = Context(
        project=project if project is not None else env.NEPTUNE_PROJECT.get(),
        api_token=api_token if api_token is not None else env.NEPTUNE_API_TOKEN.get(),
        proxies=proxies,
    )
    return _CONTEXT
