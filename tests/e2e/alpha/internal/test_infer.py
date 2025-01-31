import os
import time
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_fetcher.alpha.filter import (
    Attribute,
    ExperimentFilter,
)
from neptune_fetcher.alpha.internal.experiment import fetch_experiment_sys_attrs
from neptune_fetcher.alpha.internal.infer import (
    infer_attribute_types_in_filter,
    infer_attribute_types_in_sort_by,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TIME_NOW = time.time()
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-infer-a-{TIME_NOW}"
EXPERIMENT_NAME_B = f"pye2e-fetcher-test-internal-infer-b-{TIME_NOW}"
PATH = f"test/test-internal-infer-{TIME_NOW}"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.internal import identifiers

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            ExperimentFilter.name_in(EXPERIMENT_NAME),
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

    data = {
        f"{PATH}/int-value": 10,
        f"{PATH}/float-value": 0.5,
        f"{PATH}/str-value": "hello",
        f"{PATH}/bool-value": True,
        f"{PATH}/datetime-value": DATETIME_VALUE,
        f"{PATH}/conflicting-type-int-str-value": 10,
        f"{PATH}/conflicting-type-int-float-value": 3,
    }
    run.log_configs(data)

    path = f"{PATH}/float-series-value"
    for step, value in zip(FLOAT_SERIES_STEPS, FLOAT_SERIES_VALUES):
        run.log_metrics(data={path: value}, step=step)

    run.close()

    return run


@pytest.fixture(scope="module")
def run_with_attributes_b(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_fetcher.alpha.internal import identifiers

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            ExperimentFilter.name_in(EXPERIMENT_NAME_B),
        )
    )
    if existing.items:
        return

    run_id = str(uuid.uuid4())

    run = Run(
        project=project_identifier,
        run_id=run_id,
        experiment_name=EXPERIMENT_NAME_B,
    )

    data = {
        f"{PATH}/int-value": 10,
        f"{PATH}/float-value": 0.5,
        f"{PATH}/str-value": "hello",
        f"{PATH}/bool-value": True,
        f"{PATH}/datetime-value": DATETIME_VALUE,
        f"{PATH}/conflicting-type-int-str-value": "hello",
        f"{PATH}/conflicting-type-int-float-value": 0.3,
    }
    run.log_configs(data)

    run.close()

    return run


def test_infer_attribute_types_in_filter_no_filter(client, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    infer_attribute_types_in_filter(client, project_identifier, experiment_filter=None)

    # then
    # no exception is raised


@pytest.mark.parametrize(
    "filter_before, filter_after",
    [
        (
            ExperimentFilter.eq(f"{PATH}/int-value", 10),
            ExperimentFilter.eq(Attribute(f"{PATH}/int-value", type="int"), 10),
        ),
        (
            ExperimentFilter.eq(f"{PATH}/float-value", 0.5),
            ExperimentFilter.eq(Attribute(f"{PATH}/float-value", type="float"), 0.5),
        ),
        (
            ExperimentFilter.eq(f"{PATH}/str-value", "hello"),
            ExperimentFilter.eq(Attribute(f"{PATH}/str-value", type="string"), "hello"),
        ),
        (
            ExperimentFilter.eq(f"{PATH}/bool-value", True),
            ExperimentFilter.eq(Attribute(f"{PATH}/bool-value", type="bool"), True),
        ),
        (
            ExperimentFilter.eq(f"{PATH}/datetime-value", DATETIME_VALUE),
            ExperimentFilter.eq(Attribute(f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE),
        ),
        (
            ExperimentFilter.eq(f"{PATH}/float-series-value", FLOAT_SERIES_VALUES[-1]),
            ExperimentFilter.eq(Attribute(f"{PATH}/float-series-value", type="float_series"), FLOAT_SERIES_VALUES[-1]),
        ),
    ],
)
def test_infer_attribute_types_in_filter_single_string(
    client, project, run_with_attributes, filter_before, filter_after
):
    # given
    project_identifier = project.project_identifier

    #  when
    infer_attribute_types_in_filter(client, project_identifier, experiment_filter=filter_before)

    # then
    assert filter_before == filter_after


@pytest.mark.parametrize(
    "attribute_before, attribute_after",
    [
        (Attribute(f"{PATH}/int-value"), Attribute(f"{PATH}/int-value", type="int")),
        (Attribute(f"{PATH}/float-value"), Attribute(f"{PATH}/float-value", type="float")),
        (Attribute(f"{PATH}/str-value"), Attribute(f"{PATH}/str-value", type="string")),
        (Attribute(f"{PATH}/bool-value"), Attribute(f"{PATH}/bool-value", type="bool")),
        (Attribute(f"{PATH}/datetime-value"), Attribute(f"{PATH}/datetime-value", type="datetime")),
        (Attribute(f"{PATH}/float-series-value"), Attribute(f"{PATH}/float-series-value", type="float_series")),
    ],
)
def infer_attribute_types_in_sort_by_single(client, project, run_with_attributes, attribute_before, attribute_after):
    # given
    project_identifier = project.project_identifier

    #  when
    infer_attribute_types_in_sort_by(client, project_identifier, experiment_filter=None, sort_by=attribute_before)

    # then
    assert attribute_before == attribute_after


@pytest.mark.parametrize(
    "filter_before",
    [
        ExperimentFilter.eq(f"{PATH}/does-not-exist", 10),
    ],
)
def test_infer_attribute_types_in_filter_missing(client, project, filter_before):
    # given
    project_identifier = project.project_identifier

    #  when
    with pytest.raises(ValueError) as exc_info:
        infer_attribute_types_in_filter(client, project_identifier, experiment_filter=filter_before)

    # then
    assert "Failed to infer types for attributes" in str(exc_info.value)


@pytest.mark.parametrize(
    "attribute,experiment_filter",
    [
        (Attribute(f"{PATH}/does-not-exist"), None),
        (Attribute(f"{PATH}/does-not-exist"), ExperimentFilter.name_in(EXPERIMENT_NAME)),
        (Attribute(f"{PATH}/int-value"), ExperimentFilter.name_in(EXPERIMENT_NAME + "does-not-exist")),
    ],
)
def test_infer_attribute_types_in_sort_by_missing(client, project, attribute, experiment_filter):
    # given
    project_identifier = project.project_identifier

    #  when
    with pytest.raises(ValueError) as exc_info:
        infer_attribute_types_in_sort_by(
            client, project_identifier, experiment_filter=experiment_filter, sort_by=attribute
        )

    # then
    assert "Failed to infer types for attributes" in str(exc_info.value)


@pytest.mark.parametrize(
    "filter_before",
    [
        ExperimentFilter.eq(f"{PATH}/conflicting-type-int-str-value", 10),
        ExperimentFilter.eq(f"{PATH}/conflicting-type-int-float-value", 0.5),
    ],
)
def test_infer_attribute_types_in_filter_conflicting_types(
    client, project, run_with_attributes, run_with_attributes_b, filter_before
):
    # given
    project_identifier = project.project_identifier

    #  when
    with pytest.raises(ValueError) as exc_info:
        infer_attribute_types_in_filter(client, project_identifier, experiment_filter=filter_before)

    # then
    assert "Multiple type candidates found for attribute" in str(exc_info.value)


@pytest.mark.parametrize(
    "attribute_before,experiment_filter",
    [
        (Attribute(f"{PATH}/conflicting-type-int-str-value"), None),
        (Attribute(f"{PATH}/conflicting-type-int-float-value"), None),
        (
            Attribute(f"{PATH}/conflicting-type-int-str-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME, EXPERIMENT_NAME_B),
        ),
        (
            Attribute(f"{PATH}/conflicting-type-int-float-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME, EXPERIMENT_NAME_B),
        ),
    ],
)
def test_infer_attribute_types_in_sort_by_conflicting_types(
    client, project, run_with_attributes, run_with_attributes_b, attribute_before, experiment_filter
):
    # given
    project_identifier = project.project_identifier

    #  when
    with pytest.raises(ValueError) as exc_info:
        infer_attribute_types_in_sort_by(
            client, project_identifier, experiment_filter=experiment_filter, sort_by=attribute_before
        )

    # then
    assert "Multiple type candidates found for attribute" in str(exc_info.value)


@pytest.mark.parametrize(
    "attribute_before,experiment_filter,attribute_after",
    [
        (
            Attribute(f"{PATH}/conflicting-type-int-str-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME),
            Attribute(f"{PATH}/conflicting-type-int-str-value", type="int"),
        ),
        (
            Attribute(f"{PATH}/conflicting-type-int-str-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME_B),
            Attribute(f"{PATH}/conflicting-type-int-str-value", type="string"),
        ),
        (
            Attribute(f"{PATH}/conflicting-type-int-float-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME),
            Attribute(f"{PATH}/conflicting-type-int-float-value", type="int"),
        ),
        (
            Attribute(f"{PATH}/conflicting-type-int-float-value"),
            ExperimentFilter.name_in(EXPERIMENT_NAME_B),
            Attribute(f"{PATH}/conflicting-type-int-float-value", type="float"),
        ),
    ],
)
def test_infer_attribute_types_in_sort_by_conflicting_types_with_filter(
    client, project, run_with_attributes, run_with_attributes_b, attribute_before, experiment_filter, attribute_after
):
    # given
    project_identifier = project.project_identifier

    #  when
    infer_attribute_types_in_sort_by(
        client, project_identifier, experiment_filter=experiment_filter, sort_by=attribute_before
    )

    # then
    assert attribute_before == attribute_after
