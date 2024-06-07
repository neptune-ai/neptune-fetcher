__all__ = ["logged", "handle_critical", "StatsCollector", "TimeitDecorator"]

import pprint
import time
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    List,
    ParamSpec,
)

from neptune.internal.utils.logger import get_logger

P = ParamSpec("P")

logger = get_logger()


def logged(func: Callable[P, None]) -> Callable[P, None]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        logger.info(f"Running: {func.__name__}")
        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error running {func.__name__}: {e}")
            return
        logger.info(f"Finished: {func.__name__}")

    return wrapper


def handle_critical(func: Callable[P, None]) -> Callable[P, None]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.critical(f"Benchmarking failed: {e}")

    return wrapper


class StatsCollector:
    ROUND_N_DIGITS = 4

    def __init__(self) -> None:
        self._stats: Dict[str, List[float]] = {}

    def add_stat(self, key: str, value: float) -> None:
        value = round(value, self.ROUND_N_DIGITS)
        if key not in self._stats:
            self._stats[key] = []
        self._stats[key].append(value)

    def print_stats(self, *args: Any, **kwargs: Any) -> None:
        pprint.pprint(self._stats, *args, **kwargs)


class TimeitDecorator:
    def __init__(self, collector: StatsCollector) -> None:
        self._stats_collector = collector

    def __call__(self, func: Callable[P, None]) -> Callable[P, None]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            start = time.perf_counter()
            func(*args, **kwargs)
            end = time.perf_counter()
            self._stats_collector.add_stat(func.__name__, end - start)

        return wrapper
