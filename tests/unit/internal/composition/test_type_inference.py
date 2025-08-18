import pytest

from neptune_query.exceptions import AttributeTypeInferenceError
from neptune_query.internal.composition import type_inference
from neptune_query.internal.filters import (
    _Attribute,
    _Filter,
)


def test_attribute_inference_state_success_and_error():
    attr = _Attribute("foo")
    state = type_inference.AttributeInferenceState(attr, attr)
    assert not state.is_finalized()
    state.set_success("int", "details")
    assert state.is_finalized()
    assert state.is_inferred()
    assert state.inferred_type == "int"
    assert state.success_details == "details"
    state2 = type_inference.AttributeInferenceState(attr, attr)
    state2.set_error("err")
    assert state2.is_finalized()
    assert not state2.is_inferred()
    assert state2.error == "err"


def test_inference_state_from_attribute_and_filter():
    attr = _Attribute("foo", type="int")
    state = type_inference.InferenceState.from_attribute(attr)
    assert state.result.type == "int"
    assert state.is_complete()
    filter_ = _Filter.eq("bar", 1)
    state2 = type_inference.InferenceState.from_filter(filter_)
    assert isinstance(state2.result, _Filter)
    assert isinstance(state2.attributes, list)


def test_inference_state_incomplete_attributes_and_raise():
    attr = _Attribute("foo")
    state = type_inference.InferenceState.from_attribute(attr)
    state.attributes[0].inferred_type = None
    state.attributes[0].error = None
    assert not state.is_complete()
    with pytest.raises(AttributeTypeInferenceError):
        state.raise_if_incomplete()


def test_infer_attribute_types_locally_float_series():
    attr = _Attribute("foo", aggregation="last")
    state = type_inference.InferenceState.from_attribute(attr)
    state.attributes[0].attribute.aggregation = ["last", "min", "max", "average", "variance"]
    type_inference._infer_attribute_types_locally(state)
    assert state.attributes[0].inferred_type == "float_series"


def test_fill_unknown_types_as_string():
    attr = _Attribute("foo")
    state = type_inference.InferenceState.from_attribute(attr)
    type_inference._fill_unknown_types_as_string(state)
    assert state.attributes[0].inferred_type == "string"
    assert state.attributes[0].success_details.startswith("Defaulting")
    with pytest.warns(UserWarning, match="Attribute 'foo': Attribute doesn't exist in the project"):
        state.emit_warnings()


def test_known_attribute_types_constant():
    assert type_inference._KNOWN_SYS_ATTRIBUTES["sys/archived"] == "bool"


def test_infer_attribute_types_in_filter_empty_returns_empty():
    result = type_inference.infer_attribute_types_in_filter(
        client=None, project_identifier="proj", filter_=None, fetch_attribute_definitions_executor=None
    )
    assert result.result is None
    assert result.is_complete()


def test_infer_attribute_types_in_sort_by_complete_local():
    attr = _Attribute("sys/archived", type=None)
    result = type_inference.infer_attribute_types_in_sort_by(
        client=None, project_identifier="proj", sort_by=attr, fetch_attribute_definitions_executor=None
    )
    assert result.result.type == "bool"
    assert result.is_complete()


def test_infer_attribute_types_in_filter_local_inference():
    filter_ = _Filter.eq("sys/archived", True)
    result = type_inference.infer_attribute_types_in_filter(
        client=None, project_identifier="proj", filter_=filter_, fetch_attribute_definitions_executor=None
    )
    assert result.result.attribute.type == "bool"
    assert result.is_complete()


def test_infer_attribute_types_in_sort_by_local_inference():
    attr = _Attribute("sys/archived")
    result = type_inference.infer_attribute_types_in_sort_by(
        client=None, project_identifier="proj", sort_by=attr, fetch_attribute_definitions_executor=None
    )
    assert result.result.type == "bool"
    assert result.is_complete()
