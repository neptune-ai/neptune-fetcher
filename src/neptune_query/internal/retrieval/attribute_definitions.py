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
import itertools as it
import re
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
    Union,
)

from neptune_api.api.retrieval import query_attribute_definitions_within_project
from neptune_api.client import AuthenticatedClient
from neptune_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)

from .. import filters  # noqa: E402
from .. import (  # noqa: E402
    env,
    identifiers,
)
from ..retrieval import attribute_types as types  # noqa: E402
from ..retrieval import util  # noqa: E402
from ..retrieval import retry


def split_attribute_filters(
    _attribute_filter: filters._BaseAttributeFilter,
) -> list[filters._AttributeFilter]:
    if isinstance(_attribute_filter, filters._AttributeFilter):
        return [_attribute_filter]
    elif isinstance(_attribute_filter, filters._AttributeFilterAlternative):
        return list(it.chain.from_iterable(split_attribute_filters(child) for child in _attribute_filter.filters))
    else:
        raise RuntimeError(f"Unexpected filter type: {type(_attribute_filter)}")


def fetch_attribute_definitions_single_filter(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._AttributeFilter,
    batch_size: int = env.NEPTUNE_FETCHER_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[identifiers.AttributeDefinition], None, None]:
    params: dict[str, Any] = {
        "projectIdentifiers": list(project_identifiers),
        "attributeNameFilter": dict(),
        "nextPage": {"limit": batch_size},
    }

    if run_identifiers is not None:
        params["experimentIdsFilter"] = [str(e) for e in run_identifiers]

    # Convert name_eq to an additional condition added to each of must_match_any alternatives.
    name_regexes = None
    if attribute_filter.name_eq is not None:
        name_regexes = _escape_name_eq(_variants_to_list(attribute_filter.name_eq))

    if attribute_filter.must_match_any is not None:
        attribute_name_filter_dtos = []
        for alternative in attribute_filter.must_match_any:
            attribute_name_filter_dto = {}
            must_match_regexes = _union_options([name_regexes, alternative.must_match_regexes])
            if must_match_regexes is not None:
                attribute_name_filter_dto["mustMatchRegexes"] = must_match_regexes
            if alternative.must_not_match_regexes is not None:
                attribute_name_filter_dto["mustNotMatchRegexes"] = alternative.must_not_match_regexes
            if attribute_name_filter_dto:
                attribute_name_filter_dtos.append(attribute_name_filter_dto)
        params["attributeNameFilter"]["mustMatchAny"] = attribute_name_filter_dtos

    elif name_regexes is not None:
        params["attributeNameFilter"]["mustMatchAny"] = [{"mustMatchRegexes": name_regexes}]

    attribute_types = _variants_to_list(attribute_filter.type_in)
    if attribute_types is not None:
        params["attributeFilter"] = [
            {"attributeType": types.map_attribute_type_python_to_backend(_type)} for _type in attribute_types
        ]

    # note: attribute_filter.aggregations is intentionally ignored

    return util.fetch_pages(
        client=client,
        fetch_page=_fetch_attribute_definitions_page,
        process_page=_process_attribute_definitions_page,
        make_new_page_params=ft.partial(_make_new_attribute_definitions_page_params, batch_size=batch_size),
        params=params,
    )


def _fetch_attribute_definitions_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> QueryAttributeDefinitionsResultDTO:
    body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

    response = retry.handle_errors_default(query_attribute_definitions_within_project.sync_detailed)(
        client=client,
        body=body,
    )

    return response.parsed


def _process_attribute_definitions_page(
    data: QueryAttributeDefinitionsResultDTO,
) -> util.Page[identifiers.AttributeDefinition]:
    items = []
    for entry in data.entries:
        item = identifiers.AttributeDefinition(
            name=entry.name,
            type=types.map_attribute_type_backend_to_python(str(entry.type)),
        )
        items.append(item)
    return util.Page(items=items)


def _make_new_attribute_definitions_page_params(
    params: dict[str, Any],
    data: Optional[QueryAttributeDefinitionsResultDTO],
    batch_size: int,
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.next_page.next_page_token
    if not next_page_token or len(data.entries) < batch_size:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params


def _escape_name_eq(names: Optional[list[str]]) -> Optional[list[str]]:
    if names is None:
        return None

    escaped = [f"{re.escape(name)}" for name in names]

    if len(escaped) == 1:
        return [f"^{escaped[0]}$"]
    else:
        joined = "|".join(escaped)
        return [f"^({joined})$"]


def _variants_to_list(param: Union[str, Iterable[str], None]) -> Optional[list[str]]:
    if param is None:
        return None
    if isinstance(param, str):
        return [param]
    return list(param)


def _union_options(options: list[Optional[list[str]]]) -> Optional[list[str]]:
    result = None

    for option in options:
        if option is not None:
            if result is None:
                result = []
            result.extend(option)

    return result
