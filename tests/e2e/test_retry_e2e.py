import os

from neptune_fetcher.internal.identifiers import (
    AttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_fetcher.internal.retrieval.attribute_values import (
    AttributeValue,
    fetch_attribute_values,
)
from tests.e2e.conftest import extract_pages

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


def test_fetch_single_attribute(client, project):
    # given
    repeat = 200
    project_identifier = project.project_identifier
    experiment_identifier = RunIdentifier(project_identifier=project_identifier, sys_id=SysId("RP-1"))

    #  when
    results = []
    for i in range(repeat):
        print(f"Iteration {i + 1}/{repeat}")
        result = list(
            fetch_attribute_values(
                client,
                project_identifier,
                [experiment_identifier],
                [AttributeDefinition("sys/name", "string")],
            )
        )
        results.append(result)

    # then
    values = [extract_pages(result) for result in results]
    assert values == [
        [AttributeValue(AttributeDefinition("sys/name", "string"), "LONG-REALISTIC-001", experiment_identifier)]
        for _ in range(repeat)
    ]
