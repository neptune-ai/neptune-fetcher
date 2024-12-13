from typing import Any


class NeptuneException(Exception):
    def __eq__(self, other: Any) -> bool:
        if type(other) is type(self):
            return super().__eq__(other) and str(self).__eq__(str(other))
        else:
            return False

    def __hash__(self) -> int:
        return hash((super().__hash__(), str(self)))
