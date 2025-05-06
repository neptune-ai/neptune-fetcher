import itertools
import random
import uuid
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)

from neptune_scale.types import File

TEST_DATA_VERSION = "2025-05-07"
PATH = f"test/test-alpha-{TEST_DATA_VERSION}"
FLOAT_SERIES_PATHS = [f"{PATH}/metrics/float-series-value_{j}" for j in range(5)]
STRING_SERIES_PATHS = [f"{PATH}/metrics/string-series-value_{j}" for j in range(2)]
NUMBER_OF_STEPS = 10


@dataclass
class ExperimentData:
    name: str
    config: dict[str, any]
    string_sets: dict[str, list[str]]
    float_series: dict[str, list[float]]
    unique_series: dict[str, list[float]]
    string_series: dict[str, list[str]]
    files: dict[str, bytes]
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
            )
        )


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

                if i == 0:
                    files = {
                        f"{PATH}/files/file-value": b"Binary content",
                        f"{PATH}/files/file-value.txt": File(b"Text content", mime_type="text/plain"),
                        f"{PATH}/files/object-does-not-exist": File(
                            "/tmp/object-does-not-exist", mime_type="text/plain", size=1
                        ),
                    }

                    long_path_prefix = f"{PATH}/long/int-value-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = 1000 - long_path_prefix_len
                    long_path_configs = {f"{long_path_prefix}{k:0{k_len}d}": k for k in range(4000)}

                    long_path_prefix = f"{PATH}/long/intstring-series-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = 1000 - long_path_prefix_len
                    long_path_series = {f"{long_path_prefix}{k:0{k_len}d}": f"string-{k}" for k in range(4000)}

                    long_path_prefix = f"{PATH}/long/float-series-"
                    long_path_prefix_len = len(long_path_prefix)
                    k_len = 1000 - long_path_prefix_len
                    long_path_metrics = {f"{long_path_prefix}{k:0{k_len}d}": float(k) for k in range(4000)}
                else:
                    files = {}
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
