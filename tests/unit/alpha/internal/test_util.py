import os

import pytest

from neptune_fetcher.alpha.internal import (
    attribute,
    identifiers,
)
from neptune_fetcher.alpha.internal.env import NEPTUNE_FETCHER_QUERY_SIZE_LIMIT
from neptune_fetcher.alpha.internal.util import (
    split_experiments,
    split_experiments_attributes,
)

EXPERIMENT_IDENTIFIER = identifiers.ExperimentIdentifier(
    identifiers.ProjectIdentifier("project/abc"), identifiers.SysId("XXX-1")
)
ATTRIBUTE_DEFINITION = attribute.AttributeDefinition("config/attribute", "string")

_EXPERIMENT_SIZE = 50


@pytest.mark.parametrize(
    "experiment_identifiers, expected",
    [
        ([], [[]]),
        ([EXPERIMENT_IDENTIFIER], [[EXPERIMENT_IDENTIFIER]]),
        ([EXPERIMENT_IDENTIFIER, EXPERIMENT_IDENTIFIER], [[EXPERIMENT_IDENTIFIER, EXPERIMENT_IDENTIFIER]]),
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
        (3, _EXPERIMENT_SIZE * 2, [2, 1]),
        (4, _EXPERIMENT_SIZE * 2, [2, 2]),
        (5, _EXPERIMENT_SIZE * 2, [2, 2, 1]),
        (9, _EXPERIMENT_SIZE * 3 - 1, [2, 2, 2, 2, 1]),
        (9, _EXPERIMENT_SIZE * 3, [3, 3, 3]),
        (9, _EXPERIMENT_SIZE * 3 + 1, [3, 3, 3]),
        (9, _EXPERIMENT_SIZE * 4, [3, 3, 3]),  # 4 -> split into 3 batches -> 3, 3, 3 is more balanced than 4, 4, 1
        (8, _EXPERIMENT_SIZE * 4, [4, 4]),
        (8, _EXPERIMENT_SIZE * 3, [3, 3, 2]),
        (7, _EXPERIMENT_SIZE * 3, [3, 3, 1]),
        (6, _EXPERIMENT_SIZE * 3, [3, 3]),
        (5, _EXPERIMENT_SIZE * 3, [3, 2]),
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
