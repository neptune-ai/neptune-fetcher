import os
import re
from abc import ABC
from concurrent.futures import (
    Executor,
    ThreadPoolExecutor,
)
from dataclasses import dataclass
from typing import (
    Callable,
    Literal,
    Optional,
    Union,
)

from neptune_api.client import AuthenticatedClient
from neptune_retrieval_api.api.default import query_attribute_definitions_within_project
from neptune_retrieval_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)

from .retry import backoff_retry

__ALL__ = (
    "AttributeFilter",
    "find_attributes",
)

_DEFAULT_BATCH_SIZE = 1000
_DEFAULT_MAX_WORKERS = 10


class AttributeFilter(ABC):
    def __call__(self, **kwargs) -> "AttributeFilterSingle":
        return AttributeFilterSingle(**kwargs)

    def __or__(self, other: "AttributeFilter") -> "AttributeFilter":
        return self.any(self, other)

    def any(*filters: "AttributeFilter") -> "AttributeFilter":
        return AttributeFilterAlternative(*filters)


@dataclass
class AttributeFilterSingle(AttributeFilter):
    name_eq: Union[str, list[str], None] = None
    type_in: Optional[list[Literal["float", "int", "string", "datetime", "float_series"]]] = None
    name_matches_all: Union[str, list[str], None] = None
    name_matches_none: Union[str, list[str], None] = None
    aggregations: list[Literal["last", "min", "max", "average", "variance", "auto"]] | None = None


@dataclass
class AttributeFilterAlternative(AttributeFilter):
    filters: list[AttributeFilter]


def find_attributes(
    client: AuthenticatedClient,
    project_ids: list[str],
    experiment_ids: list[str],
    attribute_filter: AttributeFilter,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    executor: Optional[Executor] = None
) -> list[str]:
    if isinstance(attribute_filter, AttributeFilterSingle):
        return _find_attributes_single(
            client=client,
            project_ids=project_ids,
            experiment_ids=experiment_ids,
            attribute_filter=attribute_filter,
            batch_size=batch_size,
        )
    if isinstance(attribute_filter, AttributeFilterAlternative):
        def go(child: AttributeFilter) -> list[str]:
            return find_attributes(
                client=client,
                project_ids=project_ids,
                experiment_ids=experiment_ids,
                attribute_filter=child,
                batch_size=batch_size,
                executor=executor,
            )

        if executor is None:
            with _create_executor() as executor:
                results = executor.map(go, attribute_filter.filters)
        else:
            results = executor.map(go, attribute_filter.filters)
        return list(set().union(*results))


def _create_executor() -> Executor:
    max_workers = int(os.getenv("NEPTUNE_FETCHER_MAX_WORKERS", _DEFAULT_MAX_WORKERS))
    return ThreadPoolExecutor(max_workers=max_workers)


def _find_attributes_single(
    client: AuthenticatedClient,
    project_ids: list[str],
    experiment_ids: list[str],
    attribute_filter: AttributeFilterSingle,
    batch_size: int
) -> list[str]:

    params = {
        "projectIdentifiers": project_ids,
        "experimentIdsFilter": experiment_ids,
        "attributeNameFilter": {},
        "nextPage": {"limit": batch_size},
    }

    must_match_regexes = _union_options(
        [
            _map(_variants_to_list(attribute_filter.name_eq), _escape_name_eq),
            _variants_to_list(attribute_filter.name_matches_all),
        ]
    )
    if must_match_regexes is not None:
        params["attributeNameFilter"]["mustMatchRegexes"] = must_match_regexes

    must_not_match_regexes = _variants_to_list(attribute_filter.name_matches_none)
    if must_not_match_regexes is not None:
        params["attributeNameFilter"]["mustNotMatchRegexes"] = must_not_match_regexes

    attribute_types = _variants_to_list(attribute_filter.type_in)
    if attribute_types is not None:
        params["attributeFilter"] = [{"attributeType": t} for t in attribute_types]

    if attribute_filter.aggregations is not None:
        pass  # TODO: what is the intended behavior here? Is it missing from the api?

    body = QueryAttributeDefinitionsBodyDTO.from_dict(params)

    response = backoff_retry(lambda: query_attribute_definitions_within_project.sync_detailed(client=client, body=body))

    data: QueryAttributeDefinitionsResultDTO = response.parsed
    # TODO: data.next_page
    return [entry.name for entry in data.entries]


def _escape_name_eq(name: str) -> str:
    return f"^{re.escape(name)}$"


def _variants_to_list(param: Union[str, list[str], None]) -> Optional[list[str]]:
    if param is None:
        return None
    if isinstance(param, str):
        return [param]
    return param


def _map(value: Optional[list[str]], func: Callable[[str], str]) -> Optional[list[str]]:
    if value is None:
        return None
    return [func(value) for value in value]


def _union_options(options: list[Optional[list[str]]]) -> Optional[list[str]]:
    result = None

    for option in options:
        if option is not None:
            if result is None:
                result = []
            result = result.extend(option)

    return result
