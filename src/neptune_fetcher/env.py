import os
from typing import (
    Callable,
    Generic,
    TypeVar,
)

__all__ = (
    "NEPTUNE_API_TOKEN",
    "NEPTUNE_PROJECT",
    "NEPTUNE_VERIFY_SSL",
)

T = TypeVar("T")


class EnvVariable(Generic[T]):
    _UNSET: T = object()  # type: ignore

    def __init__(self, name: str, value_mapper: Callable[[str], T], default_value: T = _UNSET):
        self._name = name
        self._value_mapper = staticmethod(value_mapper)
        self._default_value = default_value

    def get(self) -> T:
        value = os.getenv(self._name)
        if value is None:
            if self._default_value is self._UNSET:
                raise ValueError(f"Environment variable {self._name} is not set")
            return self._default_value
        return self._map_value(value)

    def _map_value(self, value: str) -> T:
        return self._value_mapper(value)


def _map_str(value: str) -> str:
    return value.strip()


def _map_bool(value: str) -> bool:
    return value.lower() in {"true", "1"}


NEPTUNE_API_TOKEN = EnvVariable[str]("NEPTUNE_API_TOKEN", _map_str)
NEPTUNE_PROJECT = EnvVariable[str]("NEPTUNE_PROJECT", _map_str)
NEPTUNE_VERIFY_SSL = EnvVariable[bool]("NEPTUNE_VERIFY_SSL", _map_bool, True)
