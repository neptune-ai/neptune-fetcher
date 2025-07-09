import os
from datetime import (
    datetime,
    timezone,
)

import pytest

from neptune_query.exceptions import AttributeTypeInferenceError
from neptune_query.internal.composition.type_inference import (
    infer_attribute_types_in_filter,
    infer_attribute_types_in_sort_by,
)
from neptune_query.internal.filters import (
    _Attribute,
    _Filter,
)
from neptune_query.internal.retrieval.search import (
    ContainerType,
    fetch_experiment_sys_attrs,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
TEST_DATA_VERSION = "2025-01-31"
EXPERIMENT_NAME = f"pye2e-fetcher-test-internal-composition-type-inference-a-{TEST_DATA_VERSION}"
EXPERIMENT_NAME_B = f"pye2e-fetcher-test-internal-composition-type-inference-b-{TEST_DATA_VERSION}"
PATH = f"test/test-internal-infer-{TEST_DATA_VERSION}"
DATETIME_VALUE = datetime(2025, 1, 1, 0, 0, 0, 0, timezone.utc)
FLOAT_SERIES_STEPS = [step * 0.5 for step in range(10)]
FLOAT_SERIES_VALUES = [float(step**2) for step in range(10)]


@pytest.fixture(scope="module")
def run_with_attributes(client, project):
    import uuid

    from neptune_scale import Run

    from neptune_query.internal import identifiers

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            _Filter.name_eq(EXPERIMENT_NAME),
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

    from neptune_query.internal import identifiers

    project_identifier = project.project_identifier

    existing = next(
        fetch_experiment_sys_attrs(
            client,
            identifiers.ProjectIdentifier(project_identifier),
            _Filter.name_eq(EXPERIMENT_NAME_B),
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


def test_infer_attribute_types_in_filter_no_filter(client, executor, project, run_with_attributes):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(client, project_identifier, None, executor, executor)

    # then
    assert not result.is_run_domain_empty()
    # no exception is raised
    result.raise_if_incomplete()


@pytest.mark.parametrize(
    "filter_before, filter_after",
    [
        (
            _Filter.eq(f"{PATH}/int-value", 10),
            _Filter.eq(_Attribute(f"{PATH}/int-value", type="int"), 10),
        ),
        (
            _Filter.eq(f"{PATH}/float-value", 0.5),
            _Filter.eq(_Attribute(f"{PATH}/float-value", type="float"), 0.5),
        ),
        (
            _Filter.eq(f"{PATH}/str-value", "hello"),
            _Filter.eq(_Attribute(f"{PATH}/str-value", type="string"), "hello"),
        ),
        (
            _Filter.eq(f"{PATH}/bool-value", True),
            _Filter.eq(_Attribute(f"{PATH}/bool-value", type="bool"), True),
        ),
        (
            _Filter.eq(f"{PATH}/datetime-value", DATETIME_VALUE),
            _Filter.eq(_Attribute(f"{PATH}/datetime-value", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq(f"{PATH}/float-series-value", FLOAT_SERIES_VALUES[-1]),
            _Filter.eq(_Attribute(f"{PATH}/float-series-value", type="float_series"), FLOAT_SERIES_VALUES[-1]),
        ),
    ],
)
def test_infer_attribute_types_in_filter_single(
    client, executor, project, run_with_attributes, filter_before, filter_after
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_before,
        executor,
        executor,
    )

    # then
    assert filter_before != filter_after
    assert result.result == filter_after
    assert not result.is_run_domain_empty()
    result.raise_if_incomplete()


@pytest.mark.parametrize(
    "filter_before, filter_after",
    [
        (
            _Filter.eq("sys/name", "n"),
            _Filter.eq(_Attribute("sys/name", type="string"), "n"),
        ),
        (
            _Filter.eq("sys/id", "id"),
            _Filter.eq(_Attribute("sys/id", type="string"), "id"),
        ),
        (
            _Filter.eq("sys/modification_time", DATETIME_VALUE),
            _Filter.eq(_Attribute("sys/modification_time", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq("sys/owner", "owner"),
            _Filter.eq(_Attribute("sys/owner", type="string"), "owner"),
        ),
        (
            _Filter.eq("sys/family", "family"),
            _Filter.eq(_Attribute("sys/family", type="string"), "family"),
        ),
        (
            _Filter.eq("sys/custom_run_id", "custom_run_id"),
            _Filter.eq(_Attribute("sys/custom_run_id", type="string"), "custom_run_id"),
        ),
        (
            _Filter.eq("sys/archived", True),
            _Filter.eq(_Attribute("sys/archived", type="bool"), True),
        ),
        (
            _Filter.eq("sys/creation_time", DATETIME_VALUE),
            _Filter.eq(_Attribute("sys/creation_time", type="datetime"), DATETIME_VALUE),
        ),
        (
            _Filter.eq("sys/experiment/name", "experiment_name"),
            _Filter.eq(_Attribute("sys/experiment/name", type="string"), "experiment_name"),
        ),
        (
            _Filter.eq("sys/experiment/running_time_seconds", 10.0),
            _Filter.eq(_Attribute("sys/experiment/running_time_seconds", type="float"), 10.0),
        ),
    ],
)
def test_infer_attribute_types_in_filter_sys(
    client, executor, project, run_with_attributes, filter_before, filter_after
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_before,
        executor,
        executor,
    )

    # then
    assert filter_before != filter_after
    assert result.result == filter_after
    assert not result.is_run_domain_empty()
    result.raise_if_incomplete()
    assert all(attr.success_details == "Inferred as a known system attribute" for attr in result.attributes)


@pytest.mark.parametrize(
    "attribute_before, attribute_after",
    [
        (_Attribute(f"{PATH}/int-value"), _Attribute(f"{PATH}/int-value", type="int")),
        (_Attribute(f"{PATH}/float-value"), _Attribute(f"{PATH}/float-value", type="float")),
        (_Attribute(f"{PATH}/str-value"), _Attribute(f"{PATH}/str-value", type="string")),
        (_Attribute(f"{PATH}/bool-value"), _Attribute(f"{PATH}/bool-value", type="bool")),
        (_Attribute(f"{PATH}/datetime-value"), _Attribute(f"{PATH}/datetime-value", type="datetime")),
        (_Attribute(f"{PATH}/float-series-value"), _Attribute(f"{PATH}/float-series-value", type="float_series")),
    ],
)
def test_infer_attribute_types_in_sort_by_single(
    client, executor, project, run_with_attributes, attribute_before, attribute_after
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=None,
        sort_by=attribute_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
    )

    # then
    assert attribute_before != attribute_after
    assert result.result == attribute_after
    assert not result.is_run_domain_empty()
    result.raise_if_incomplete()


@pytest.mark.parametrize(
    "filter_before",
    [
        _Filter.eq(f"{PATH}/does-not-exist", 10),
    ],
)
def test_infer_attribute_types_in_filter_missing(client, executor, project, filter_before):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_=filter_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    assert "could not find the attribute" in str(exc.value)


@pytest.mark.parametrize(
    "attribute,experiment_filter",
    [
        (_Attribute(f"{PATH}/does-not-exist"), None),
        (_Attribute(f"{PATH}/does-not-exist"), _Filter.name_eq(EXPERIMENT_NAME)),
    ],
)
def test_infer_attribute_types_in_sort_by_missing_attribute(client, executor, project, attribute, experiment_filter):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=experiment_filter,
        sort_by=attribute,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    assert "could not find the attribute" in str(exc.value)


@pytest.mark.parametrize(
    "attribute,experiment_filter",
    [
        (_Attribute(f"{PATH}/does-not-exist"), _Filter.name_eq(EXPERIMENT_NAME + "does-not-exist")),
        (_Attribute(f"{PATH}/int-value"), _Filter.name_eq(EXPERIMENT_NAME + "does-not-exist")),
    ],
)
def test_infer_attribute_types_in_sort_by_missing_experiment(client, executor, project, attribute, experiment_filter):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=experiment_filter,
        sort_by=attribute,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
    )

    # then
    assert result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    assert "could not find the attribute" in str(exc.value)


@pytest.mark.parametrize(
    "filter_before",
    [
        _Filter.eq(f"{PATH}/conflicting-type-int-str-value", 10),
    ],
)
def test_infer_attribute_types_in_filter_conflicting_types_int_string(
    client, executor, project, run_with_attributes, run_with_attributes_b, filter_before
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_=filter_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple experiments with conflicting types: (int, string|string, int)"
    )


@pytest.mark.skip(
    reason="Backend inconsistently skips one of the two records (int/float). Merge with the test above when fixed"
)
@pytest.mark.parametrize(
    "filter_before",
    [
        _Filter.eq(f"{PATH}/conflicting-type-int-float-value", 0.5),
    ],
)
def test_infer_attribute_types_in_filter_conflicting_types_int_float(
    client, executor, project, run_with_attributes, run_with_attributes_b, filter_before
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_filter(
        client,
        project_identifier,
        filter_=filter_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple experiments with conflicting types: (int, float|float, int)"
    )


@pytest.mark.parametrize(
    "attribute_before,experiment_filter",
    [
        (_Attribute(f"{PATH}/conflicting-type-int-str-value"), None),
        (
            _Attribute(f"{PATH}/conflicting-type-int-str-value"),
            _Filter.any([_Filter.name_eq(EXPERIMENT_NAME), _Filter.name_eq(EXPERIMENT_NAME_B)]),
        ),
    ],
)
def test_infer_attribute_types_in_sort_by_conflicting_types_int_string(
    client, executor, project, run_with_attributes, run_with_attributes_b, attribute_before, experiment_filter
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=experiment_filter,
        sort_by=attribute_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple experiments with conflicting types: (int, string|string, int)"
    )


@pytest.mark.parametrize(
    "attribute_before,experiment_filter",
    [
        (_Attribute(f"{PATH}/conflicting-type-int-float-value"), None),
        (
            _Attribute(f"{PATH}/conflicting-type-int-float-value"),
            _Filter.any([_Filter.name_eq(EXPERIMENT_NAME), _Filter.name_eq(EXPERIMENT_NAME_B)]),
        ),
    ],
)
def test_infer_attribute_types_in_sort_by_conflicting_types_int_float(
    client, executor, project, run_with_attributes, run_with_attributes_b, attribute_before, experiment_filter
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=experiment_filter,
        sort_by=attribute_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    assert not result.is_run_domain_empty()
    with pytest.raises(AttributeTypeInferenceError) as exc:
        result.raise_if_incomplete()
    exc.match(
        "Neptune found the attribute name in multiple experiments with conflicting types: (int, float|float, int)"
    )


@pytest.mark.parametrize(
    "attribute_before,experiment_filter,attribute_after",
    [
        (
            _Attribute(f"{PATH}/conflicting-type-int-str-value"),
            _Filter.name_eq(EXPERIMENT_NAME),
            _Attribute(f"{PATH}/conflicting-type-int-str-value", type="int"),
        ),
        (
            _Attribute(f"{PATH}/conflicting-type-int-str-value"),
            _Filter.name_eq(EXPERIMENT_NAME_B),
            _Attribute(f"{PATH}/conflicting-type-int-str-value", type="string"),
        ),
        (
            _Attribute(f"{PATH}/conflicting-type-int-float-value"),
            _Filter.name_eq(EXPERIMENT_NAME),
            _Attribute(f"{PATH}/conflicting-type-int-float-value", type="int"),
        ),
        (
            _Attribute(f"{PATH}/conflicting-type-int-float-value"),
            _Filter.name_eq(EXPERIMENT_NAME_B),
            _Attribute(f"{PATH}/conflicting-type-int-float-value", type="float"),
        ),
    ],
)
def test_infer_attribute_types_in_sort_by_conflicting_types_with_filter(
    client,
    executor,
    project,
    run_with_attributes,
    run_with_attributes_b,
    attribute_before,
    experiment_filter,
    attribute_after,
):
    # given
    project_identifier = project.project_identifier

    #  when
    result = infer_attribute_types_in_sort_by(
        client,
        project_identifier,
        filter_=experiment_filter,
        sort_by=attribute_before,
        executor=executor,
        fetch_attribute_definitions_executor=executor,
    )

    # then
    assert attribute_before != attribute_after
    assert result.result == attribute_after
    assert not result.is_run_domain_empty()
    result.raise_if_incomplete()
