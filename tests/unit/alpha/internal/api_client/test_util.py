import os

import pytest

from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.api_client.attribute_definitions import AttributeDefinition
from neptune_fetcher.alpha.internal.api_client.util import (
    split_experiments,
    split_experiments_attributes,
)
from neptune_fetcher.alpha.internal.env import (
    NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE,
    NEPTUNE_FETCHER_QUERY_SIZE_LIMIT,
)

EXPERIMENT_IDENTIFIERS = [
    identifiers.ExperimentIdentifier(identifiers.ProjectIdentifier("project/abc"), identifiers.SysId(f"XXX-{n}"))
    for n in range(10)
]
EXPERIMENT_IDENTIFIER = EXPERIMENT_IDENTIFIERS[0]
ATTRIBUTE_DEFINITIONS = [AttributeDefinition(f"config/attribute{n}", "string") for n in range(10)]
ATTRIBUTE_DEFINITION = ATTRIBUTE_DEFINITIONS[0]
ATTRIBUTE_DEFINITION_SIZES = [len(attr.name) for attr in ATTRIBUTE_DEFINITIONS]
ATTRIBUTE_DEFINITION_SIZE = ATTRIBUTE_DEFINITION_SIZES[0]

EXPERIMENT_SIZE = 50


@pytest.mark.parametrize(
    "experiment_identifiers, expected",
    [
        ([], [[]]),
        ([EXPERIMENT_IDENTIFIER], [[EXPERIMENT_IDENTIFIER]]),
        (
            [EXPERIMENT_IDENTIFIERS[0], EXPERIMENT_IDENTIFIERS[1]],
            [[EXPERIMENT_IDENTIFIERS[0], EXPERIMENT_IDENTIFIERS[1]]],
        ),
    ],
)
def test_split_experiments(experiment_identifiers, expected):
    # when
    groups = list(split_experiments(experiment_identifiers=experiment_identifiers))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "given_num, query_size_limit, expected_nums",
    [
        (0, 0, [0]),
        (1, 0, [1]),
        (2, 0, [1, 1]),
        (3, EXPERIMENT_SIZE * 2, [2, 1]),
        (4, EXPERIMENT_SIZE * 2, [2, 2]),
        (5, EXPERIMENT_SIZE * 2, [2, 2, 1]),
        (9, EXPERIMENT_SIZE * 3 - 1, [2, 2, 2, 2, 1]),
        (9, EXPERIMENT_SIZE * 3, [3, 3, 3]),
        (9, EXPERIMENT_SIZE * 3 + 1, [3, 3, 3]),
        (9, EXPERIMENT_SIZE * 4, [3, 3, 3]),  # 4 -> split into 3 batches -> 3, 3, 3 is more balanced than 4, 4, 1
        (8, EXPERIMENT_SIZE * 4, [4, 4]),
        (8, EXPERIMENT_SIZE * 3, [3, 3, 2]),
        (7, EXPERIMENT_SIZE * 3, [3, 3, 1]),
        (6, EXPERIMENT_SIZE * 3, [3, 3]),
        (5, EXPERIMENT_SIZE * 3, [3, 2]),
    ],
)
def test_split_experiments_custom_envs(given_num, query_size_limit, expected_nums):
    # given
    experiment_identifiers = [EXPERIMENT_IDENTIFIER] * given_num
    expected = [[EXPERIMENT_IDENTIFIER] * num for num in expected_nums]
    os.environ[NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.name] = str(query_size_limit)

    # when
    groups = list(split_experiments(experiment_identifiers=experiment_identifiers))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "experiments, attributes, expected",
    [
        ([], [], []),
        ([EXPERIMENT_IDENTIFIER], [], []),
        ([], [ATTRIBUTE_DEFINITION], []),
        ([EXPERIMENT_IDENTIFIER], [ATTRIBUTE_DEFINITION], [([EXPERIMENT_IDENTIFIER], [ATTRIBUTE_DEFINITION])]),
        ([EXPERIMENT_IDENTIFIER] * 2, [ATTRIBUTE_DEFINITION], [([EXPERIMENT_IDENTIFIER] * 2, [ATTRIBUTE_DEFINITION])]),
        ([EXPERIMENT_IDENTIFIER], [ATTRIBUTE_DEFINITION] * 2, [([EXPERIMENT_IDENTIFIER], [ATTRIBUTE_DEFINITION] * 2)]),
        (
            [EXPERIMENT_IDENTIFIER] * 2,
            [ATTRIBUTE_DEFINITION] * 2,
            [([EXPERIMENT_IDENTIFIER] * 2, [ATTRIBUTE_DEFINITION] * 2)],
        ),
    ],
)
def test_split_experiments_attributes(experiments, attributes, expected):
    # when
    groups = list(split_experiments_attributes(experiment_identifiers=experiments, attribute_definitions=attributes))

    # then
    assert groups == expected


@pytest.mark.parametrize(
    "experiment_num, attribute_num, query_size_limit, values_batch_size, expected_nums",
    [
        (1, 1, 500, 500, [(1, 1)]),
        (1, 1, 1, 500, [(1, 1)]),
        (1, 1, 500, 1, [(1, 1)]),
        (1, 1, 1, 1, [(1, 1)]),
        (2, 3, 500, 500, [(2, 3)]),
        (2, 3, 500, 1, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 1, 500, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 1, 1, [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)]),
        (2, 3, 500, 2, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, 500, 3, [(1, 3), (1, 3)]),
        (2, 3, 500, 4, [(1, 3), (1, 3)]),
        (2, 3, 500, 5, [(1, 3), (1, 3)]),
        (2, 3, 500, 6, [(2, 3)]),
        (3, 3, 500, 6, [(2, 3), (1, 3)]),
        (2, 3, EXPERIMENT_SIZE + 2 * ATTRIBUTE_DEFINITION_SIZE, 500, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, EXPERIMENT_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE - 1, 500, [(1, 2), (1, 1), (1, 2), (1, 1)]),
        (2, 3, EXPERIMENT_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE, 500, [(1, 3), (1, 3)]),
        (2, 3, 2 * EXPERIMENT_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE - 1, 500, [(1, 3), (1, 3)]),
        (2, 3, 2 * EXPERIMENT_SIZE + 3 * ATTRIBUTE_DEFINITION_SIZE, 500, [(2, 3)]),
    ],
)
def test_split_experiments_attributes_custom_envs(
    experiment_num, attribute_num, query_size_limit, values_batch_size, expected_nums
):
    experiments = [EXPERIMENT_IDENTIFIER] * experiment_num
    attributes = [ATTRIBUTE_DEFINITION] * attribute_num
    expected = [([EXPERIMENT_IDENTIFIER] * a, [ATTRIBUTE_DEFINITION] * b) for a, b in expected_nums]
    os.environ[NEPTUNE_FETCHER_QUERY_SIZE_LIMIT.name] = str(query_size_limit)
    os.environ[NEPTUNE_FETCHER_ATTRIBUTE_VALUES_BATCH_SIZE.name] = str(values_batch_size)

    # when
    groups = list(split_experiments_attributes(experiment_identifiers=experiments, attribute_definitions=attributes))

    # then
    assert groups == expected
