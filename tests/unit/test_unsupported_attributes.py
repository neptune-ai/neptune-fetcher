import uuid
import warnings
from datetime import datetime
from typing import (
    Iterable,
    List,
    Optional,
)
from unittest.mock import (
    Mock,
    patch,
)

import neptune_retrieval_api
import pytest
from neptune_api.models import ProjectDTO
from neptune_api.types import Response
from neptune_retrieval_api.models import QueryAttributesBodyDTO
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.attributes_pb2 import (
    ProtoQueryAttributesExperimentResultDTO,
    ProtoQueryAttributesResultDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoAttributesDTO,
    ProtoFloatSeriesAttributeDTO,
    ProtoLeaderboardEntriesSearchResultDTO,
)
from pytest import fixture

from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)
from neptune_fetcher.fetchable import SUPPORTED_TYPES
from neptune_fetcher.fields import (
    FieldDefinition,
    FieldType,
    FloatPointValue,
)
from neptune_fetcher.util import NeptuneWarning

# IMPORTANT:
# 1) The mocked API calls used in the tests always add two unsupported attributes to the result by default,
#    unless the test explicitly provides a different list of unsupported attributes (or none at all).
# 2) The tests know which attributes to expect, based on make_proto_attributes_dto() and the default unsupported
#    attributes
# 3) We treat all warnings as errors in this test suite (see warnings_are_errors()), unless pytest.warns with
#    warnings.simplefilter('once') is used explicitly.
#    Because of this tests like test_list_runs() might seem like they don't do anything special, but they do make
#    sure than no unnecessary warnings are issued.

DEFAULT_UNSUPPORTED_ATTRS = (("badAttr", "typeFoo"), ("anotherAttr", "typeBar"))
RUN_ID = str(uuid.uuid4())


def make_attr(name, value=None, typ=None) -> ProtoAttributeDTO:
    """Return an attribute definition with an optional value. Based on the type (or type of value),
    dynamically determines which protobuf objects to instantiate and which fields to set
    in the resulting ProtoAttributeDTO object.
    """

    if typ is None:
        assert value is not None, "value must be provided if type is not specified"
        if isinstance(value, str):
            typ = "string"
        elif isinstance(value, int):
            typ = "int"
        elif isinstance(value, float):
            typ = "float"
        else:
            raise ValueError(f"Unsupported type {type(value)}. Fix your tests or add support for this type.")

    # string -> ProtoStringAttributeDTO. For unknown attrs we default to None, which
    # means value will not be set

    kwargs = {
        "name": name,
        "type": typ,
    }

    cls_name = f"Proto{typ.capitalize()}AttributeDTO"
    # Special case
    if typ == "floatSeries":
        assert value is not None, "value must be provided for floatSeries"
        kwargs["float_series_properties"] = ProtoFloatSeriesAttributeDTO(
            attribute_name=name, attribute_type=typ, last=value
        )
        return ProtoAttributeDTO(**kwargs)

    cls = getattr(neptune_retrieval_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2, cls_name, None)
    if cls is None and value is not None:
        raise ValueError("You provided a value for an unsupported type. Fix your test.")

    if value is not None and cls is not None:
        # Set the proper field based on the type
        kwargs[f"{typ}_properties"] = cls(attribute_name=name, attribute_type=typ, value=value)

    return ProtoAttributeDTO(**kwargs)


def make_proto_attributes_dto(
    unsupported_attrs: Optional[Iterable[tuple[str, str]]] = DEFAULT_UNSUPPORTED_ATTRS
) -> ProtoAttributesDTO:
    """Return a list valid attributes for a run. Optionally add the provided unsupported attributes to the result,
    which default to DEFAULT_UNSUPPORTED_ATTRS."""

    unsupported_attrs = unsupported_attrs or []
    attributes = [make_attr(name, typ=type_) for name, type_ in unsupported_attrs]
    attributes.append(make_attr("sys/id", RUN_ID))
    attributes.append(make_attr("sys/custom_run_id", RUN_ID))
    attributes.append(make_attr("metric", 42, "floatSeries"))

    return ProtoAttributesDTO(experiment_id=RUN_ID, type="run", attributes=attributes)


def response(body: ProtoAttributesDTO) -> Response:
    return Mock(status_code=Mock(value=200), content=body.SerializeToString())


class MockApiClient(Mock):
    # Each (attribute_name, attribute_type) tuple will be used to add attributes
    # to the result in methods that list attributes. Set it to empty list or None
    # to only return valid attributes
    unsupported_attrs = DEFAULT_UNSUPPORTED_ATTRS

    def project_name_lookup(self, name: str) -> ProjectDTO:
        return ProjectDTO("project", "workspace", 1, "project-id", "TEST", "org_id")

    def search_entries(self, project_id, body) -> ProtoLeaderboardEntriesSearchResultDTO:
        result = make_proto_attributes_dto(self.unsupported_attrs)
        # Just return a single result
        return ProtoLeaderboardEntriesSearchResultDTO(matching_item_count=1, entries=[result])

    def query_attribute_definitions(self, container_id: str) -> List[FieldDefinition]:
        result = [FieldDefinition(f"attr-{tp}", FieldType(tp.value)) for tp in SUPPORTED_TYPES]
        result.append(FieldDefinition("sys/id", FieldType("string")))

        unsupported_attrs = self.unsupported_attrs or []
        for name, type_ in unsupported_attrs:
            result.append(FieldDefinition(name, FieldType(type_)))

        return result

    def query_attributes_within_project(
        self, project_id: str, body: QueryAttributesBodyDTO
    ) -> ProtoQueryAttributesResultDTO:
        attributes = make_proto_attributes_dto(self.unsupported_attrs).attributes
        entries = [
            ProtoQueryAttributesExperimentResultDTO(
                experimentId=RUN_ID, experimentShortId="RUN-1337", attributes=attributes
            )
        ]
        return ProtoQueryAttributesResultDTO(entries=entries)

    def fetch_multiple_series_values(self, *args, **kwargs) -> list[(str, List[FloatPointValue])]:
        points = [FloatPointValue(datetime.now(), 42, 1)]
        return [("metric", points)]


@fixture
def get_attributes_with_paths_filter_proto():
    # FieldsCache uses neptune_api directly, so we cannot mock on TestApiClient level
    with patch("neptune_retrieval_api.api.default.get_attributes_with_paths_filter_proto.sync_detailed") as patched:
        yield patched


@fixture(autouse=True)
def warnings_are_errors():
    # By default, we will treat all warnings as errors, unless pytest.warns is used
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        yield


@fixture
def backend_cls():
    with patch("neptune_fetcher.read_only_project.ApiClient", new=MockApiClient) as patched:
        yield patched


@fixture
def project(backend_cls):
    return ReadOnlyProject("workspace/project")


def _assert_warning(record, *attr_types):
    """Assert that a warning about unsupported types, for a given type, was issued exactly once."""
    if not attr_types:
        # Default to the two unsupported types used in TestApiClient
        attr_types = [type_ for _, type_ in DEFAULT_UNSUPPORTED_ATTRS]

    attr_types = set(attr_types)
    # Assume warnings.simplefilter('once') is used
    assert len(record) == len(attr_types)

    # Make sure there is a warning for each expected type
    for attr_type in attr_types:
        for rec in record:
            msg = rec.message.args[0]
            if f"of type `{attr_type}`" in msg and "not supported" in msg:
                break
        else:
            assert False, f"Expected a warning for type `{attr_type}`"


# ReadOnlyRun tests


def test_run_no_warning_when_attribute_type_is_known(project, get_attributes_with_paths_filter_proto):
    project._backend.unsupported_attrs = None
    attrs = make_proto_attributes_dto(None)
    get_attributes_with_paths_filter_proto.return_value = response(attrs)

    run = ReadOnlyRun(project, RUN_ID)
    for a in attrs.attributes:
        run[a.name].fetch()

    run = ReadOnlyRun(project, RUN_ID, eager_load_fields=False)
    for a in attrs.attributes:
        run[a.name].fetch()


def test_field_names(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        field_names = set(run.field_names)

        # bad attrs should still be present in field names
        assert "badAttr" in field_names
        assert "anotherAttr" in field_names

    _assert_warning(record)

    project._backend.unsupported_attrs = None

    run = ReadOnlyRun(project, RUN_ID)
    field_names = set(run.field_names)

    assert "badAttr" not in field_names
    assert "anotherAttr" not in field_names


def test_run_eager_load_attributes(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

    _assert_warning(record)


def test_run_no_eager_load_attributes(project, get_attributes_with_paths_filter_proto):
    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto(DEFAULT_UNSUPPORTED_ATTRS))

    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID, eager_load_fields=False)
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

    _assert_warning(record)


def test_run_fetch_missing_attribute(project, get_attributes_with_paths_filter_proto):
    """Fetch an attribute that does not exist, then make it exist and fetch it again."""
    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto(None))
    project._backend.unsupported_attrs = None

    with pytest.raises(KeyError):
        run = ReadOnlyRun(project, RUN_ID)
        run["badAttr"].fetch()
        run["anotherAttr"].fetch()

    project._backend.unsupported_attrs = DEFAULT_UNSUPPORTED_ATTRS
    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto(DEFAULT_UNSUPPORTED_ATTRS))

    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        assert run["badAttr"].fetch() is None
        assert run["anotherAttr"].fetch() is None

    _assert_warning(record)


def test_prefetch(project, get_attributes_with_paths_filter_proto):
    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto())
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch(["badAttr", "anotherAttr", "sys/id"])
        assert run["badAttr"].fetch() is None
        assert run["anotherAttr"].fetch() is None
        assert run["sys/id"].fetch() == RUN_ID

    _assert_warning(record)

    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto(None))
    run.prefetch(["badAttr", "anotherAttr", "sys/id"])


def test_prefetch_series_values(project, get_attributes_with_paths_filter_proto):
    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto())
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch_series_values(["badAttr", "anotherAttr", "metric"])
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

        assert run["metric"].fetch_last() == 42

    _assert_warning(record)

    get_attributes_with_paths_filter_proto.return_value = response(make_proto_attributes_dto(None))
    project._backend.unsupported_attrs = None

    with pytest.raises(KeyError):
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch_series_values(["badAttr", "anotherAttr", "metric"])

        run["badAttr"].fetch_values()
        run["anotherAttr"].fetch_values()


# ReadOnlyProject tests


def test_fetch_runs_df(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        df = project.fetch_runs_df(columns_regex=".*")
        assert df.shape[0] == 1, "Only one run should be returned"
        row = df.iloc[0]
        assert row["sys/id"] == RUN_ID
        assert row["badAttr"] is None
        assert row["anotherAttr"] is None

    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warnings should be issued
    df = project.fetch_runs_df(columns_regex=".*")
    assert df.shape[0] == 1, "Only one run should be returned"
    row = df.iloc[0]
    assert row["sys/id"] == RUN_ID
    assert "badAttr" not in row
    assert "anotherAttr" not in row


def test_fetch_experiments_df(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        df = project.fetch_experiments_df(columns_regex=".*")
        assert df.shape[0] == 1, "Only one experiment should be returned"
        row = df.iloc[0]
        assert row["sys/id"] == RUN_ID
        assert row["badAttr"] is None
        assert row["anotherAttr"] is None

    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warnings should be issued
    df = project.fetch_experiments_df(columns_regex=".*")
    assert df.shape[0] == 1, "Only one experiment should be returned"
    row = df.iloc[0]
    assert row["sys/id"] == RUN_ID
    assert "badAttr" not in row
    assert "anotherAttr" not in row


def test_fetch_runs(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        df = project.fetch_runs()
        assert df.shape[0] == 1, "Only one run should be returned"
    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warning should be issued
    df = project.fetch_runs()
    assert df.shape[0] == 1, "Only one run should be returned"


def test_fetch_experiments(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        df = project.fetch_experiments()
        assert df.shape[0] == 1, "Only one run should be returned"
    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warning should be issued
    df = project.fetch_experiments()
    assert df.shape[0] == 1, "Only one run should be returned"


def test_fetch_read_only_runs(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        assert len(list(project.fetch_read_only_runs(custom_ids=[RUN_ID]))) == 1

    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warning should be issued
    assert len(list(project.fetch_read_only_runs(custom_ids=[RUN_ID]))) == 1


def test_fetch_read_only_experiments(project):
    with pytest.warns(NeptuneWarning) as record:
        warnings.simplefilter("once")
        assert len(list(project.fetch_read_only_experiments(names=["does-not-matter"]))) == 1

    _assert_warning(record)

    project._backend.unsupported_attrs = None
    # No warning should be issued
    assert len(list(project.fetch_read_only_experiments(names=["does-not-matter"]))) == 1


def test_list_runs(project):
    # list_runs() filters on sys attributes, with a hard assumption that they're strings,
    # so it doesn't care about the unsupported ones. No warning should be issued.
    assert len(list(project.list_runs())) == 1


def test_list_experiments(project):
    # list_experiments() filters on sys attributes, with a hard assumption that they're strings,
    # so it doesn't care about the unsupported ones. No warning should be issued.
    assert len(list(project.list_experiments())) == 1
