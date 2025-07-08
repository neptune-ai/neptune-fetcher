#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools as ft
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generator,
    List,
    Literal,
    Optional,
    Protocol,
    TypeVar,
)

from neptune_api.api.retrieval import search_leaderboard_entries_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import SearchLeaderboardEntriesParamsDTO
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import ProtoLeaderboardEntriesSearchResultDTO

from .. import (
    env,
    identifiers,
)
from ..filters import (
    _Attribute,
    _Filter,
)
from ..retrieval import (
    retry,
    util,
)
from ..retrieval.attribute_types import map_attribute_type_python_to_backend

_DIRECTION_PYTHON_TO_BACKEND_MAP: dict[str, str] = {
    "asc": "ascending",
    "desc": "descending",
}


def _map_direction(direction: Literal["asc", "desc"]) -> str:
    return _DIRECTION_PYTHON_TO_BACKEND_MAP[direction]


T = TypeVar("T")


class ContainerType(Enum):
    RUN = "run"
    EXPERIMENT = "experiment"


class SysIdLabel(Protocol):
    @property
    def sys_id(self) -> identifiers.SysId:
        ...

    @property
    def label(self) -> str:
        ...


@dataclass(frozen=True)
class ExperimentSysAttrs:
    sys_id: identifiers.SysId
    sys_name: identifiers.SysName

    @property
    def label(self) -> str:
        return self.sys_name

    @staticmethod
    def attribute_names() -> List[str]:
        return ["sys/name", "sys/id"]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ExperimentSysAttrs":
        return ExperimentSysAttrs(
            sys_name=identifiers.SysName(data["sys/name"]),
            sys_id=identifiers.SysId(data["sys/id"]),
        )


@dataclass(frozen=True)
class RunSysAttrs:
    sys_id: identifiers.SysId
    sys_custom_run_id: identifiers.CustomRunId

    @property
    def label(self) -> str:
        return self.sys_custom_run_id

    @staticmethod
    def attribute_names() -> List[str]:
        return ["sys/custom_run_id", "sys/id"]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "RunSysAttrs":
        return RunSysAttrs(
            sys_custom_run_id=identifiers.CustomRunId(data["sys/custom_run_id"]),
            sys_id=identifiers.SysId(data["sys/id"]),
        )


def _sys_id_from_dict(data: dict[str, Any]) -> identifiers.SysId:
    return identifiers.SysId(data["sys/id"])


class FetchSysAttrs(Protocol[T]):
    def __call__(
        self,
        client: AuthenticatedClient,
        project_identifier: identifiers.ProjectIdentifier,
        filter_: Optional[_Filter] = None,
        sort_by: _Attribute = _Attribute("sys/creation_time", type="datetime"),
        sort_direction: Literal["asc", "desc"] = "desc",
        limit: Optional[int] = None,
        batch_size: int = env.NEPTUNE_FETCHER_SYS_ATTRS_BATCH_SIZE.get(),
        container_type: ContainerType = ContainerType.EXPERIMENT,
    ) -> Generator[util.Page[T], None, None]:
        ...


def _create_fetch_sys_attrs(
    attribute_names: List[str],
    make_record: Callable[[dict[str, Any]], T],
    default_container_type: ContainerType,
) -> FetchSysAttrs[T]:
    def fetch_sys_attrs(
        client: AuthenticatedClient,
        project_identifier: identifiers.ProjectIdentifier,
        filter_: Optional[_Filter] = None,
        sort_by: _Attribute = _Attribute("sys/creation_time", type="datetime"),
        sort_direction: Literal["asc", "desc"] = "desc",
        limit: Optional[int] = None,
        batch_size: int = env.NEPTUNE_FETCHER_SYS_ATTRS_BATCH_SIZE.get(),
        container_type: ContainerType = default_container_type,
    ) -> Generator[util.Page[T], None, None]:
        params: dict[str, Any] = {
            "attributeFilters": [{"path": attribute_name} for attribute_name in attribute_names],
            "pagination": {"limit": batch_size},
            "experimentLeader": container_type == ContainerType.EXPERIMENT,
            "sorting": {
                "dir": _map_direction(sort_direction),
                "sortBy": {"name": sort_by.name},
            },
        }
        if filter_ is not None:
            params["query"] = {"query": str(filter_)}
        if sort_by.aggregation is not None:
            params["sorting"]["aggregationMode"] = sort_by.aggregation
        if sort_by.type is not None:
            params["sorting"]["sortBy"]["type"] = map_attribute_type_python_to_backend(sort_by.type)

        return util.fetch_pages(
            client=client,
            fetch_page=ft.partial(_fetch_sys_attrs_page, project_identifier=project_identifier),
            process_page=ft.partial(_process_sys_attrs_page, make_record=make_record),
            make_new_page_params=ft.partial(_make_new_sys_attrs_page_params, batch_size=batch_size, limit=limit),
            params=params,
        )

    return fetch_sys_attrs


fetch_experiment_sys_attrs = _create_fetch_sys_attrs(
    attribute_names=ExperimentSysAttrs.attribute_names(),
    make_record=ExperimentSysAttrs.from_dict,
    default_container_type=ContainerType.EXPERIMENT,
)


fetch_run_sys_attrs = _create_fetch_sys_attrs(
    attribute_names=RunSysAttrs.attribute_names(),
    make_record=RunSysAttrs.from_dict,
    default_container_type=ContainerType.RUN,
)


def fetch_sys_id_labels(container_type: ContainerType) -> FetchSysAttrs[SysIdLabel]:
    if container_type == ContainerType.EXPERIMENT:
        return fetch_experiment_sys_attrs  # type: ignore
    elif container_type == ContainerType.RUN:
        return fetch_run_sys_attrs  # type: ignore
    else:
        raise RuntimeError(f"Unexpected container type: {container_type}")


fetch_experiment_sys_ids = _create_fetch_sys_attrs(
    attribute_names=["sys/id"], make_record=_sys_id_from_dict, default_container_type=ContainerType.EXPERIMENT
)

fetch_run_sys_ids = _create_fetch_sys_attrs(
    attribute_names=["sys/id"],
    make_record=_sys_id_from_dict,
    default_container_type=ContainerType.RUN,
)

fetch_sys_ids = fetch_experiment_sys_ids


def _fetch_sys_attrs_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project_identifier: identifiers.ProjectIdentifier,
) -> ProtoLeaderboardEntriesSearchResultDTO:
    body = SearchLeaderboardEntriesParamsDTO.from_dict(params)

    response = retry.handle_errors_default(search_leaderboard_entries_proto.sync_detailed)(
        client=client,
        project_identifier=project_identifier,
        type=["run"],
        body=body,
    )

    return ProtoLeaderboardEntriesSearchResultDTO.FromString(response.content)


def _process_sys_attrs_page(
    data: ProtoLeaderboardEntriesSearchResultDTO,
    make_record: Callable[[dict[str, Any]], T],
) -> util.Page[T]:
    items = []
    for entry in data.entries:
        attributes = {attr.name: attr.string_properties.value for attr in entry.attributes}
        item = make_record(attributes)
        items.append(item)
    return util.Page(items=items)


def _make_new_sys_attrs_page_params(
    params: dict[str, Any],
    data: Optional[ProtoLeaderboardEntriesSearchResultDTO],
    batch_size: int,
    limit: Optional[int],
) -> Optional[dict[str, Any]]:

    if data is None:
        params["pagination"]["offset"] = 0
        if limit is not None:
            params["pagination"]["limit"] = min(limit, batch_size)
        return params

    if len(data.entries) < batch_size:
        return None

    params["pagination"]["offset"] += batch_size
    if limit is not None:
        offset = params["pagination"]["offset"]
        params["pagination"]["limit"] = min(limit - offset, batch_size)

    return params
