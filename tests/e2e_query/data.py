import itertools
import random
import re
import uuid
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)
from typing import Any

from neptune_scale.types import File as ScaleFile
from neptune_scale.types import Histogram as ScaleHistogram

from neptune_query.internal.retrieval.attribute_types import Histogram as FetcherHistogram
from neptune_query.types import Histogram as OHistogram

TEST_DATA_VERSION = "2025-07-14"
PATH = f"test/test-alpha-{TEST_DATA_VERSION}"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]
STRING_SERIES_PATHS = [f"{PATH}/metrics/string-series-value_{j}" for j in range(2)]
FILE_SERIES_PATHS = [f"{PATH}/files/file-series-value_{j}" for j in range(2)]
HISTOGRAM_SERIES_PATHS = [f"{PATH}/metrics/histogram-series-value_{j}" for j in range(3)]
NUMBER_OF_STEPS = 10
MAX_PATH_LENGTH = 1024
FILE_SERIES_STEPS = 3


@dataclass
class PathMatcher:
    pattern: str

    def __eq__(self, other):
        if isinstance(other, str):
            return re.search(self.pattern, other) is not None
        elif isinstance(other, PathMatcher):
            return self.pattern == other.pattern
        else:
            raise TypeError(f"Cannot compare PathMatcher with {type(other)}")


@dataclass
class FileMatcher:
    path_pattern: str
    size_bytes: int
    mime_type: str

    @property
    def path(self) -> PathMatcher:
        return PathMatcher(self.path_pattern)

    def __eq__(self, other):
        return (
            self.size_bytes == other.size_bytes
            and self.mime_type == other.mime_type
            and re.search(self.path_pattern, other.path) is not None
        )


@dataclass
class ExperimentData:
    name: str
    config: dict[str, Any]
    string_sets: dict[str, list[str]]
    float_series: dict[str, list[float]]
    unique_series: dict[str, list[float]]
    string_series: dict[str, list[str]]
    files: dict[str, bytes]
    file_series: dict[str, list[bytes]]
    histogram_series: dict[str, list[ScaleHistogram]]
    long_path_configs: dict[str, int]
    long_path_series: dict[str, str]
    long_path_metrics: dict[str, float]
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def all_attribute_names(self) -> set[str]:
        return set(
            itertools.chain(
                self.config.keys(),
                self.string_sets.keys(),
                self.float_series.keys(),
                self.unique_series.keys(),
                self.string_series.keys(),
                self.files.keys(),
                self.file_series.keys(),
                self.histogram_series.keys(),
                self.long_path_configs.keys(),
                self.long_path_series.keys(),
                self.long_path_metrics.keys(),
            )
        )

    def fetcher_histogram_series(self) -> dict[str, list[FetcherHistogram]]:
        return {
            key: [FetcherHistogram(type="COUNTING", edges=value.bin_edges, values=value.counts) for value in values]
            for key, values in self.histogram_series.items()
        }

    def output_histogram_series(self) -> dict[str, list[FetcherHistogram]]:
        return {
            key: [OHistogram(type="COUNTING", edges=value.bin_edges, values=value.counts) for value in values]
            for key, values in self.histogram_series.items()
        }

    def file_series_matchers(self) -> dict[str, list[FileMatcher]]:
        return {
            key: [
                FileMatcher(
                    path_pattern=key.rsplit("/", 1)[-1], mime_type="application/octet-stream", size_bytes=len(value)
                )
                for value in values
            ]
            for key, values in self.file_series.items()
        }


@dataclass
class TestData:
    experiments: list[ExperimentData] = field(default_factory=list)

    def exp_name(self, index: int) -> str:
        return self.experiments[index].name

    def __post_init__(self):
        random.seed(10)

        if not self.experiments:
            for i in range(6):
                experiment_name = f"test_alpha_{i}_{TEST_DATA_VERSION}"
                config = {
                    f"{PATH}/int-value": i,
                    f"{PATH}/float-value": float(i),
                    f"{PATH}/str-value": f"hello_{i}",
                    f"{PATH}/bool-value": i % 2 == 0,
                    f"{PATH}/datetime-value": datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc),
                    f"{PATH}/unique-value-{i}": f"unique_{i}",
                }

                string_sets = {f"{PATH}/string_set-value": [f"string-{i}-{j}" for j in range(5)]}
                float_series = {
                    path: [float(step**2) + float(random.uniform(0, 1)) for step in range(NUMBER_OF_STEPS)]
                    for path in FLOAT_SERIES_PATHS
                }

                float_series[f"{PATH}/metrics/step"] = [float(step) for step in range(NUMBER_OF_STEPS)]

                string_series = {
                    path: [f"string-{i}-{j}" for j in range(NUMBER_OF_STEPS)] for path in STRING_SERIES_PATHS
                }

                histogram_series = {
                    path: [
                        ScaleHistogram(
                            bin_edges=[n + j for n in range(6)],
                            counts=[n * j for n in range(5)],
                        )
                        for j in range(NUMBER_OF_STEPS)
                    ]
                    for path in HISTOGRAM_SERIES_PATHS
                }

                if i == 0:
                    files = {
                        f"{PATH}/files/file-value": b"Binary content",
                        f"{PATH}/files/file-value.txt": ScaleFile(b"Text content", mime_type="text/plain"),
                        f"{PATH}/files/object-does-not-exist": ScaleFile(
                            "/tmp/object-does-not-exist", mime_type="text/plain", size=1
                        ),
                    }

                    file_series = {
                        path: [f"file-{i}-{j}".encode("utf-8") for j in range(FILE_SERIES_STEPS)]
                        for path in FILE_SERIES_PATHS
                    }
                else:
                    files = {}
                    file_series = {}

                if i <= 2:
                    long_path_prefix = f"{PATH}/long/int-value-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = MAX_PATH_LENGTH - long_path_prefix_len
                    long_path_configs = {f"{long_path_prefix}{k:0{k_len}d}": k for k in range(4000)}

                    long_path_prefix = f"{PATH}/long/string-series-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = MAX_PATH_LENGTH - long_path_prefix_len
                    long_path_series = {f"{long_path_prefix}{k:0{k_len}d}": f"string-{k}" for k in range(4000)}

                    long_path_prefix = f"{PATH}/long/float-series-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = MAX_PATH_LENGTH - long_path_prefix_len
                    long_path_metrics = {f"{long_path_prefix}{k:0{k_len}d}": float(k) for k in range(4000)}
                else:
                    long_path_configs = {}
                    long_path_series = {}
                    long_path_metrics = {}

                self.experiments.append(
                    ExperimentData(
                        name=experiment_name,
                        config=config,
                        string_sets=string_sets,
                        float_series=float_series,
                        string_series=string_series,
                        file_series=file_series,
                        histogram_series=histogram_series,
                        unique_series={},
                        long_path_configs=long_path_configs,
                        long_path_series=long_path_series,
                        long_path_metrics=long_path_metrics,
                        files=files,
                    )
                )

    @property
    def experiment_names(self):
        return [exp.name for exp in self.experiments]

    @property
    def all_attribute_names(self):
        return set(itertools.chain.from_iterable(exp.all_attribute_names for exp in self.experiments))


TEST_DATA = TestData()
NOW = datetime(2025, 4, 22, 15, 30, 11, 1, timezone.utc)
