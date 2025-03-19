import itertools
from dataclasses import dataclass

import pytest

from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.composition.attribute_components import fetch_attribute_values_split
from neptune_fetcher.alpha.internal.identifiers import RunIdentifier
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition

TEST_DATA_VERSION = "2025-03-11"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-composition-attribute-components-{TEST_DATA_VERSION}"
COMMON_PATH = f"test/test-internal-composition-attribute-components-{TEST_DATA_VERSION}"


@dataclass
class TestData:
    configs: dict[str, int]
    metrics: dict[str, float]


@pytest.fixture(scope="module")
def test_data():
    return TestData(
        configs={f"{COMMON_PATH}/config_{i:05}" + "A" * 512: i for i in range(1000)},
        metrics={f"{COMMON_PATH}/metric_{i:05}" + "A" * 512: float(i) for i in range(1000)},
    )


@pytest.fixture(scope="module")
def run_with_attributes(client, project, test_data):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.alpha.internal import identifiers
    from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            Filter.name_in(EXPERIMENT_NAME),
        )
    )
    if existing.items:
        return

    run_id = str(uuid.uuid4())

    run = Run(
        project=project_identifier,
        run_id=run_id,
        experiment_name=EXPERIMENT_NAME,
    )

    run.log_configs(test_data.configs)
    run.log_metrics(test_data.metrics, step=1)
    run.close()

    return run


@pytest.fixture(scope="module")
def experiment_identifier(client, project, run_with_attributes) -> RunIdentifier:
    from neptune_fetcher.alpha.filters import Filter
    from neptune_fetcher.alpha.internal.retrieval.search import fetch_experiment_sys_attrs

    project_identifier = project.project_identifier

    experiment_filter = Filter.name_in(EXPERIMENT_NAME)
    page = next(fetch_experiment_sys_attrs(client, project_identifier=project_identifier, filter_=experiment_filter))
    sys_id = page.items[0].sys_id

    return RunIdentifier(project_identifier, sys_id)


def test_fetch_attribute_values_split_with_long_attribute_names(
    client, project, executor, test_data, experiment_identifier
):
    """Verify that properly split a single request for attributes with long names."""

    definitions = [AttributeDefinition(name=name, type="int") for name in test_data.configs] + [
        AttributeDefinition(name=name, type="float_series") for name in test_data.metrics
    ]
    expected_names = {*itertools.chain(test_data.configs.keys(), test_data.metrics.keys())}

    output = fetch_attribute_values_split(
        client,
        project.project_identifier,
        executor,
        [experiment_identifier.sys_id],
        definitions,
        downstream=concurrency.return_value,
    )

    fetched_attributes = _extract_pages(concurrency.gather_results(output))
    fetched_names = {attr.attribute_definition.name for attr in fetched_attributes}

    assert fetched_names == expected_names


def _extract_pages(generator):
    return list(itertools.chain.from_iterable(i.items for i in generator))
