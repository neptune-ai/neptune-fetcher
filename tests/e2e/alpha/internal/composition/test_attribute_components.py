import itertools

from e2e.alpha.internal.conftest import extract_pages
from e2e.alpha.internal.data import TEST_DATA

from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.composition.attribute_components import fetch_attribute_values_split
from neptune_fetcher.alpha.internal.retrieval.attribute_definitions import AttributeDefinition


def test_fetch_attribute_values_split_with_long_attribute_names(client, project, executor, experiment_identifier):
    """Verify that properly split a single request for attributes with long names, which will need to be split
    because of the limits."""

    experiment = TEST_DATA.experiments[0]
    long_int_names = [name for name in experiment.config if "long-int" in name]
    metric_names = experiment.float_series.keys()

    definitions = list(
        itertools.chain(
            [AttributeDefinition(name=name, type="int") for name in long_int_names],
            [AttributeDefinition(name=name, type="float_series") for name in metric_names],
        )
    )
    expected_names = {*itertools.chain(long_int_names, metric_names)}

    output = fetch_attribute_values_split(
        client,
        project.project_identifier,
        executor,
        [experiment_identifier.sys_id],
        definitions,
        downstream=concurrency.return_value,
    )

    fetched_attributes = extract_pages(concurrency.gather_results(output))
    fetched_names = {attr.attribute_definition.name for attr in fetched_attributes}

    assert len(fetched_names) == len(expected_names)
    assert fetched_names == expected_names
