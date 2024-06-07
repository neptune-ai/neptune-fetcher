import inspect
import sys
from typing import Any

from constants import (
    FETCH_DF_PROJECT,
    FETCH_FLOAT_PROJECT,
    FETCH_FLOAT_RUN_ID,
    LIST_RUNS_PROJECT,
)
from neptune.internal.utils.logger import get_logger
from utils import (
    StatsCollector,
    TimeitDecorator,
    handle_critical,
    logged,
)

from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)

float_series_project = ReadOnlyProject(project=FETCH_FLOAT_PROJECT)
float_series_run = ReadOnlyRun(read_only_project=float_series_project, custom_id=FETCH_FLOAT_RUN_ID)

list_runs_project = ReadOnlyProject(project=LIST_RUNS_PROJECT)

fetch_df_project = ReadOnlyProject(project=FETCH_DF_PROJECT)

logger = get_logger()

stats_collector = StatsCollector()
timed = TimeitDecorator(collector=stats_collector)

column_filter = ["float_{}".format(i) for i in range(98)]

paths_to_prefetch = (
    ["float_{}".format(i) for i in range(2000)]
    + ["int_{}".format(i) for i in range(2000)]
    + ["string_{}".format(i) for i in range(2000)]
    + ["bool_{}".format(i) for i in range(2000)]
)


@logged
@timed
def benchmark_fetch_float_series_points() -> None:
    # fetch 1M float points
    float_series_run["series/my_float_series"].fetch_values()


@logged
@timed
def benchmark_list_runs() -> None:
    # list 20k runs
    _ = [r for r in list_runs_project.list_runs()]


@logged
@timed
def benchmark_fetch_runs_df_default() -> None:
    # 1000 runs x 10_000 fields
    fetch_df_project.fetch_runs_df()


@logged
@timed
def benchmark_fetch_runs_df_custom_columns() -> None:
    # 1000 columns
    fetch_df_project.fetch_runs_df(columns=column_filter)


@logged
@timed
def benchmark_fetch_runs_df_custom_columns_regex() -> None:
    # ~100 columns out of 10_000 and 1000 runs
    fetch_df_project.fetch_runs_df(columns=["sys/id"], columns_regex="float_..")


@logged
@timed
def benchmark_fetch_runs_df_custom_id_regex() -> None:
    # limit 100 runs  (from 1000 runs)
    fetch_df_project.fetch_runs_df(custom_id_regex=".*_[1-9]*", limit=100)


@logged
@timed
def benchmark_fetch_runs_df_complex_query() -> None:
    fetch_df_project.fetch_runs_df(
        columns=["sys/id", "sys/custom_id"],
        columns_regex="float_..",
        custom_id_regex=".*_[1-9]*",
        limit=100,
    )


@logged
@timed
def benchmark_fetch_all_metadata_from_run() -> None:
    # 1000 runs x 10_000 atom fields + series fetch last
    for run in fetch_df_project.list_runs():
        logger.info("Fetching metadata for run: %s", run["sys/id"])
        read_only_run = ReadOnlyRun(read_only_project=fetch_df_project, with_id=run["sys/id"])

        logger.info("Prefetching data for run: %s", run["sys/id"])
        read_only_run.prefetch(paths=paths_to_prefetch)
        logger.info("Prefetching data for run: %s", run["sys/id"])

        for field in paths_to_prefetch:
            read_only_run[field].fetch()


def is_benchmark_func(name: str, obj: Any) -> bool:
    return name.startswith("benchmark") and inspect.isfunction(obj)


@handle_critical
def run_benchmark() -> None:
    logger.info("Discovering benchmarks")
    benchmarks = [obj for name, obj in inspect.getmembers(sys.modules[__name__]) if is_benchmark_func(name, obj)]
    logger.info("Found %d runnable benchmarks", len(benchmarks))

    for benchmark in benchmarks:
        benchmark()


if __name__ == "__main__":
    run_benchmark()
    stats_collector.print_stats(width=1)
