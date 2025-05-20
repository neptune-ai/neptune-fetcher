import uuid
import warnings
from contextlib import contextmanager
from typing import (
    Any,
    Iterable,
    Optional,
)
from unittest.mock import (
    Mock,
    patch,
)

import pytest
from neptune_api.models import (
    AttributeDefinitionDTO,
    AttributeTypeDTO,
    NextPageDTO,
    ProjectDTO,
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)
from neptune_api.proto.neptune_pb.api.v1.model.attributes_pb2 import (
    ProtoAttributeDefinitionDTO,
    ProtoAttributesSearchResultDTO,
    ProtoQueryAttributesExperimentResultDTO,
    ProtoQueryAttributesResultDTO,
)
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributeDTO,
    ProtoAttributesDTO,
    ProtoDatetimeAttributeDTO,
    ProtoFloatSeriesAttributeDTO,
    ProtoLeaderboardEntriesSearchResultDTO,
    ProtoStringAttributeDTO,
)
from neptune_api.proto.neptune_pb.api.v1.model.series_values_pb2 import (
    ProtoFloatPointValueDTO,
    ProtoFloatSeriesValuesDTO,
    ProtoFloatSeriesValuesResponseDTO,
    ProtoFloatSeriesValuesSingleSeriesResponseDTO,
)
from neptune_api.types import Response
from pytest import fixture

import neptune_fetcher
from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)
from neptune_fetcher.api.api_client import ApiClient
from neptune_fetcher.fetchable import SUPPORTED_TYPES
from neptune_fetcher.util import (
    NeptuneWarning,
    warn_unsupported_value_type,
)

# IMPORTANT:
# - The mocked API calls used in the tests always add three unsupported attributes to the result by default,
#   unless the test explicitly provides a different list of unsupported attributes (or none at all).
#   Two attributes share a type, so we have two distinct unsupported types by default
# - some of the patched API mock objects define a "good" and "bad" response constants. These are used
#   for convenience in tests where the defaults are sufficient.
# - The tests know which attributes to expect, based on make_proto_attributes_dto() and the default unsupported
#   attributes
# - We treat all warnings as errors in this test suite (see warnings_are_errors()), unless pytest.warns with
#   warnings.simplefilter('once') is used explicitly.
#   Because of this tests like test_list_runs() might seem like they don't do anything special, but they do make
#   sure than no unnecessary warnings are issued.


# Default unsupported attributes returned in API mock functions. Note that typeFoo is used twice,
# to make sure warnings are issued for a given unsupported type only once.
DEFAULT_UNSUPPORTED_ATTRS = (("badAttr", "typeFoo"), ("anotherAttr", "typeBar"), ("badAttr2", "typeFoo"))
SUPPORTED_TYPES_STR = {type_.value for type_ in SUPPORTED_TYPES}
RUN_ID = str(uuid.uuid4())
CREATION_TIME = 1737648980


def make_proto_attributes_dto(
    unsupported_attrs: Optional[Iterable[tuple[str, str]]] = DEFAULT_UNSUPPORTED_ATTRS,
) -> ProtoAttributesDTO:
    """Return a list valid attributes for a run. Optionally add the provided unsupported attributes to the result,
    which default to DEFAULT_UNSUPPORTED_ATTRS."""

    unsupported_attrs = unsupported_attrs or []
    attributes = [ProtoAttributeDTO(name=name, type=type_) for name, type_ in unsupported_attrs]
    attributes.append(
        ProtoAttributeDTO(
            name="sys/id",
            type="string",
            string_properties=ProtoStringAttributeDTO(attribute_name="sys/id", attribute_type="string", value=RUN_ID),
        )
    )
    attributes.append(
        ProtoAttributeDTO(
            name="sys/custom_run_id",
            type="string",
            string_properties=ProtoStringAttributeDTO(
                attribute_name="sys/custom_run_id", attribute_type="string", value=RUN_ID
            ),
        )
    )
    attributes.append(
        ProtoAttributeDTO(
            name="sys/creation_time",
            type="datetime",
            datetime_properties=ProtoDatetimeAttributeDTO(
                attribute_name="sys/creation_time", attribute_type="datetime", value=CREATION_TIME
            ),
        )
    )
    attributes.append(
        ProtoAttributeDTO(
            name="series",
            type="floatSeries",
            float_series_properties=ProtoFloatSeriesAttributeDTO(
                attribute_name="sys/creation_time", attribute_type="floatSeries", last=42
            ),
        )
    )

    return ProtoAttributesDTO(experiment_id=RUN_ID, type="run", attributes=attributes)


class MockApiClient(ApiClient):
    def __init__(self, *args, **kwargs) -> None:
        # Override init to avoid an attempt to authenticate
        self._backend = None


def proto_response(body: Any) -> Response:
    return Mock(status_code=Mock(value=200), content=body.SerializeToString())


def json_response(body: Any) -> Response:
    return Mock(status_code=Mock(value=200), content=body.to_dict(), parsed=body)


@fixture(autouse=True)
def get_attributes_with_paths_filter_proto():
    with patch("neptune_retrieval_api.api.default.get_attributes_with_paths_filter_proto.sync_detailed") as patched:
        patched.return_value = proto_response(make_proto_attributes_dto())
        yield patched


@fixture(autouse=True)
def get_multiple_float_series_values_proto():
    with patch("neptune_retrieval_api.api.default.get_multiple_float_series_values_proto.sync_detailed") as patched:
        # Just return a series with a single point
        points = [ProtoFloatPointValueDTO(timestamp_millis=1, step=1, value=42)]
        values = ProtoFloatSeriesValuesDTO(total_item_count=1, values=points)
        series = ProtoFloatSeriesValuesSingleSeriesResponseDTO(requestId="1234", series=values)

        patched.return_value = proto_response(ProtoFloatSeriesValuesResponseDTO(series=[series]))
        yield patched


@fixture(autouse=True)
def query_attribute_definitions_proto():
    with patch("neptune_retrieval_api.api.default.query_attribute_definitions_proto.sync_detailed") as patched:
        entries = [ProtoAttributeDefinitionDTO(name=f"attr-{type_}", type=type_) for type_ in SUPPORTED_TYPES_STR]
        patched.SUPPORTED_TYPES_RESPONSE = proto_response(ProtoAttributesSearchResultDTO(entries=entries))

        entries = entries + [
            ProtoAttributeDefinitionDTO(name=name, type=type_) for name, type_ in DEFAULT_UNSUPPORTED_ATTRS
        ]
        patched.UNSUPPORTED_TYPES_RESPONSE = proto_response(ProtoAttributesSearchResultDTO(entries=entries))

        patched.return_value = patched.UNSUPPORTED_TYPES_RESPONSE
        yield patched


@fixture(autouse=True)
def query_attributes_within_project_proto():
    with patch("neptune_retrieval_api.api.default.query_attributes_within_project_proto.sync_detailed") as patched:
        patched.UNSUPPORTED_TYPES_RESPONSE = proto_response(
            ProtoQueryAttributesResultDTO(
                entries=[
                    ProtoQueryAttributesExperimentResultDTO(
                        experimentId=RUN_ID,
                        experimentShortId="RUN-1337",
                        attributes=make_proto_attributes_dto().attributes,
                    )
                ]
            )
        )
        patched.SUPPORTED_TYPES_RESPONSE = proto_response(
            ProtoQueryAttributesResultDTO(
                entries=[
                    ProtoQueryAttributesExperimentResultDTO(
                        experimentId=RUN_ID,
                        experimentShortId="RUN-1337",
                        attributes=make_proto_attributes_dto(None).attributes,
                    )
                ]
            )
        )

        patched.return_value = patched.UNSUPPORTED_TYPES_RESPONSE
        yield patched


@fixture(autouse=True)
def query_attribute_definitions_within_project():
    with patch("neptune_retrieval_api.api.default.query_attribute_definitions_within_project.sync_detailed") as patched:
        entries = [AttributeDefinitionDTO(f"attr-{type_}", AttributeTypeDTO(type_)) for type_ in SUPPORTED_TYPES_STR]
        # Need to use Mock for unsupported type, otherwise the Enum will raise an exception
        entries += [
            AttributeDefinitionDTO(name=name, type=Mock(value=type_)) for name, type_ in DEFAULT_UNSUPPORTED_ATTRS
        ]
        patched.return_value = json_response(
            QueryAttributeDefinitionsResultDTO(entries=entries, next_page=NextPageDTO())
        )
        yield patched

        # Validate if all types passed to filter are supported, even though it's not strictly necessary:
        # `QueryAttributeDefinitionsBodyDTO.from_dict()` (the `body` argument to this function) will raise an
        # exception anyway, if an unsupported type is provided.
        #
        # However, if at some point we regenerate `neptune-api` with the option to use raw str instead
        # of Enum, this check will grab our attention to double-check code that depends on this function.
        for call in patched.call_args_list:
            assert not call.args
            body = call.kwargs["body"]
            for attr_filter in body.attribute_filter:
                assert attr_filter.attribute_type.value in SUPPORTED_TYPES_STR


@fixture(autouse=True)
def search_leaderboard_entries_proto():
    with patch("neptune_retrieval_api.api.default.search_leaderboard_entries_proto.sync_detailed") as patched:
        entries = make_proto_attributes_dto()
        patched.UNSUPPORTED_TYPES_RESPONSE = proto_response(
            ProtoLeaderboardEntriesSearchResultDTO(matching_item_count=len(entries.attributes), entries=[entries])
        )
        entries = make_proto_attributes_dto(None)
        patched.SUPPORTED_TYPES_RESPONSE = proto_response(
            ProtoLeaderboardEntriesSearchResultDTO(matching_item_count=len(entries.attributes), entries=[entries])
        )

        patched.return_value = patched.UNSUPPORTED_TYPES_RESPONSE
        yield patched


@fixture(autouse=True)
def get_project():
    with patch("neptune_api.api.backend.get_project.sync_detailed") as patched:
        patched.return_value = json_response(ProjectDTO("project", "workspace", 1, "project-id", "TEST", "org-id"))
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
    return ReadOnlyProject("workspace/project", "token")


@contextmanager
def warns_and_forgets_types(*args, **kwargs):
    """
    A simple wrapper around pytest.warns, that makes sure we clear the set of warned types before and after the test.
    If we didn't do that, we'd get false positives in tests that expect a warning to NOT be emitted. Clearing
    before the test gives us a predictable state.

    See comments for util.py:_warned_types for more details.
    """

    neptune_fetcher.util._warned_types.clear()

    with pytest.warns(NeptuneWarning, *args, **kwargs) as record:
        yield record

    neptune_fetcher.util._warned_types.clear()


def _assert_warning(record, *attr_types):
    """Assert that a warning about unsupported types, for a given type, was issued exactly once."""

    if not attr_types:
        # Default to unsupported types used in TestApiClient
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


def test_warn_unsupported_value_type():
    """Should warn only once about a given type"""

    with pytest.warns(NeptuneWarning) as record:
        warn_unsupported_value_type("test")

    _assert_warning(record, "test")

    # No warning should be issued.
    # This will trigger an error if a warning is issued, because of the warning_are_errors() fixture
    warn_unsupported_value_type("test")


def test_warn_and_forget_type():
    with warns_and_forgets_types() as record:
        warn_unsupported_value_type("test")
        warn_unsupported_value_type("test")

    _assert_warning(record, "test")

    with warns_and_forgets_types() as record:
        warn_unsupported_value_type("test")
        warn_unsupported_value_type("test")

    _assert_warning(record, "test")


# ReadOnlyRun tests


def test_run_no_warning_when_attribute_type_is_known(
    project, query_attribute_definitions_proto, search_leaderboard_entries_proto, get_attributes_with_paths_filter_proto
):
    attrs = make_proto_attributes_dto(None)
    search_leaderboard_entries_proto.return_value = proto_response(
        ProtoLeaderboardEntriesSearchResultDTO(matching_item_count=len(attrs.attributes), entries=[attrs])
    )
    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE
    get_attributes_with_paths_filter_proto.return_value = proto_response(attrs)

    run = ReadOnlyRun(project, RUN_ID)
    for a in attrs.attributes:
        run[a.name].fetch()

    run = ReadOnlyRun(project, RUN_ID, eager_load_fields=False)
    for a in attrs.attributes:
        run[a.name].fetch()


def test_field_names(project, query_attribute_definitions_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        field_names = set(run.field_names)

        # bad attrs should still be present in field names
        assert "badAttr" in field_names
        assert "anotherAttr" in field_names

    _assert_warning(record)

    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE

    run = ReadOnlyRun(project, RUN_ID)
    field_names = set(run.field_names)

    assert "badAttr" not in field_names
    assert "anotherAttr" not in field_names


def test_run_eager_load_attributes(project):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["badAttr2"].fetch() is None
        assert run["badAttr2"].fetch_last() is None
        assert run["badAttr2"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

    _assert_warning(record)


def test_run_no_eager_load_attributes(project, get_attributes_with_paths_filter_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID, eager_load_fields=False)
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["badAttr2"].fetch() is None
        assert run["badAttr2"].fetch_last() is None
        assert run["badAttr2"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

    _assert_warning(record)


def test_run_fetch_missing_attribute(
    project, get_attributes_with_paths_filter_proto, query_attribute_definitions_proto
):
    """Fetch an unsupported attribute that does not exist, then make it exist and fetch it again."""

    get_attributes_with_paths_filter_proto.return_value = proto_response(make_proto_attributes_dto(None))
    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE

    # Attr does not exist yet
    with pytest.raises(KeyError):
        run = ReadOnlyRun(project, RUN_ID)
        run["badAttr"].fetch()
        run["anotherAttr"].fetch()

    # Now return it in the response
    get_attributes_with_paths_filter_proto.return_value = proto_response(make_proto_attributes_dto())
    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.UNSUPPORTED_TYPES_RESPONSE

    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        assert run["badAttr"].fetch() is None
        assert run["anotherAttr"].fetch() is None

    _assert_warning(record)


def test_prefetch(project, get_attributes_with_paths_filter_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch(["badAttr", "anotherAttr", "sys/id"])
        assert run["badAttr"].fetch() is None
        assert run["anotherAttr"].fetch() is None
        assert run["sys/id"].fetch() == RUN_ID

    _assert_warning(record)

    get_attributes_with_paths_filter_proto.return_value = proto_response(make_proto_attributes_dto(None))
    run.prefetch(["badAttr", "anotherAttr", "sys/id"])


def test_series_no_prefetch(project):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

        assert run["series"].fetch_last() == 42

    _assert_warning(record)


def test_prefetch_series_values(project, get_attributes_with_paths_filter_proto, query_attribute_definitions_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch_series_values(["badAttr", "anotherAttr", "series"])
        assert run["badAttr"].fetch() is None
        assert run["badAttr"].fetch_last() is None
        assert run["badAttr"].fetch_values().empty

        assert run["anotherAttr"].fetch() is None
        assert run["anotherAttr"].fetch_last() is None
        assert run["anotherAttr"].fetch_values().empty

        assert run["series"].fetch_last() == 42

    _assert_warning(record)

    get_attributes_with_paths_filter_proto.return_value = proto_response(make_proto_attributes_dto(None))
    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE

    with pytest.raises(KeyError):
        run = ReadOnlyRun(project, RUN_ID)
        run.prefetch_series_values(["badAttr", "anotherAttr", "series"])

        run["badAttr"].fetch_values()
        run["anotherAttr"].fetch_values()


# ReadOnlyProject tests


def test_fetch_runs_df(project, query_attributes_within_project_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        df = project.fetch_runs_df(columns_regex=".*")
        assert df.shape[0] == 1, "Only one run should be returned"
        row = df.iloc[0]
        assert row["sys/id"] == RUN_ID
        assert row["badAttr"] is None
        assert row["anotherAttr"] is None

    _assert_warning(record)

    query_attributes_within_project_proto.return_value = query_attributes_within_project_proto.SUPPORTED_TYPES_RESPONSE

    # No warnings should be issued
    df = project.fetch_runs_df(columns_regex=".*")
    assert df.shape[0] == 1, "Only one run should be returned"
    row = df.iloc[0]
    assert row["sys/id"] == RUN_ID
    assert "badAttr" not in row
    assert "anotherAttr" not in row


def test_fetch_runs_df_sorting_with_bad_column(
    project, search_leaderboard_entries_proto, query_attribute_definitions_within_project
):
    # ApiClient.find_field_type_within_project will query the backend for type of the sorting column,
    # filtering on a subset of known types that are viable for sorting. We should return an empty result
    # in that case.
    query_attribute_definitions_within_project.return_value = json_response(
        QueryAttributeDefinitionsResultDTO(entries=[], next_page=NextPageDTO())
    )

    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        project.fetch_runs_df(sort_by="badAttr")

    # The first warning is about the missing sorting column
    message = record.pop(NeptuneWarning).message
    assert "Could not find sorting column type for field 'badAttr'" in str(message)

    _assert_warning(record)

    # Make sure we defaulted to a known type once we couldn't find the sorting column
    query_attribute_definitions_within_project.assert_called_once()
    body: QueryAttributeDefinitionsBodyDTO = query_attribute_definitions_within_project.call_args[1]["body"]
    assert all(attr_filter.attribute_type.value in SUPPORTED_TYPES_STR for attr_filter in body.attribute_filter)


def test_fetch_experiments_df(project, query_attributes_within_project_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        df = project.fetch_experiments_df(columns_regex=".*")
        assert df.shape[0] == 1, "Only one experiment should be returned"
        row = df.iloc[0]
        assert row["sys/id"] == RUN_ID
        assert row["badAttr"] is None
        assert row["anotherAttr"] is None

    _assert_warning(record)

    query_attributes_within_project_proto.return_value = query_attributes_within_project_proto.SUPPORTED_TYPES_RESPONSE

    # No warnings should be issued
    df = project.fetch_experiments_df(columns_regex=".*")
    assert df.shape[0] == 1, "Only one experiment should be returned"
    row = df.iloc[0]
    assert row["sys/id"] == RUN_ID
    assert "badAttr" not in row
    assert "anotherAttr" not in row


def test_fetch_experiments_df_sorting_with_bad_column(
    project, search_leaderboard_entries_proto, query_attribute_definitions_within_project
):
    # ApiClient.find_field_type_within_project will query the backend for type of the sorting column,
    # filtering on a subset of known types that are viable for sorting. We should return an empty result
    # in that case.
    query_attribute_definitions_within_project.return_value = json_response(
        QueryAttributeDefinitionsResultDTO(entries=[], next_page=NextPageDTO())
    )

    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        project.fetch_runs_df(sort_by="badAttr")

    # The first warning is about the missing sorting column
    message = record.pop(NeptuneWarning).message
    assert "Could not find sorting column type for field 'badAttr'" in str(message)

    _assert_warning(record)

    # Make sure we defaulted to a known type once we couldn't find the sorting column
    query_attribute_definitions_within_project.assert_called_once()
    body: QueryAttributeDefinitionsBodyDTO = query_attribute_definitions_within_project.call_args[1]["body"]
    assert all(attr_filter.attribute_type.value in SUPPORTED_TYPES_STR for attr_filter in body.attribute_filter)


def test_fetch_runs(project, query_attributes_within_project_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        df = project.fetch_runs()
        assert df.shape[0] == 1, "Only one run should be returned"
    _assert_warning(record)

    query_attributes_within_project_proto.return_value = query_attributes_within_project_proto.SUPPORTED_TYPES_RESPONSE

    # No warning should be issued
    df = project.fetch_runs()
    assert df.shape[0] == 1, "Only one run should be returned"


def test_fetch_experiments(project, query_attributes_within_project_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        df = project.fetch_experiments()
        assert df.shape[0] == 1, "Only one run should be returned"
    _assert_warning(record)

    query_attributes_within_project_proto.return_value = query_attributes_within_project_proto.SUPPORTED_TYPES_RESPONSE

    # No warning should be issued
    df = project.fetch_experiments()
    assert df.shape[0] == 1, "Only one run should be returned"


def test_fetch_read_only_runs(project, query_attribute_definitions_proto, search_leaderboard_entries_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        assert len(list(project.fetch_read_only_runs(custom_ids=[RUN_ID]))) == 1

    _assert_warning(record)

    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE
    search_leaderboard_entries_proto.return_value = search_leaderboard_entries_proto.SUPPORTED_TYPES_RESPONSE

    # No warning should be issued
    assert len(list(project.fetch_read_only_runs(custom_ids=[RUN_ID]))) == 1


def test_fetch_read_only_experiments(project, query_attribute_definitions_proto, search_leaderboard_entries_proto):
    with warns_and_forgets_types() as record:
        warnings.simplefilter("once")
        assert len(list(project.fetch_read_only_experiments(names=["does-not-matter"]))) == 1

    _assert_warning(record)

    query_attribute_definitions_proto.return_value = query_attribute_definitions_proto.SUPPORTED_TYPES_RESPONSE
    search_leaderboard_entries_proto.return_value = search_leaderboard_entries_proto.SUPPORTED_TYPES_RESPONSE

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
