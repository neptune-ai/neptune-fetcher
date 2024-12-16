from typing import (
    Any,
    Optional,
    Union,
)

from neptune_retrieval_api.models import SearchLeaderboardEntriesParamsDTO

from ..context import (
    Context,
    get_context,
)
from ..filter import ExperimentFilter
from .auth import create_authenticated_client
from .client import NeptuneApiClient


class NeptuneFetcher:
    def __init__(self, project_id: str, client: NeptuneApiClient):
        self._project_id = project_id
        self._client = client

    @staticmethod
    def create(context: Optional[Context] = None) -> "NeptuneFetcher":
        if context is None:
            context = get_context()
        auth_client = create_authenticated_client(api_token=context.api_token, proxies=context.proxies)
        client = NeptuneApiClient(auth_client=auth_client)
        return NeptuneFetcher(project_id=context.project, client=client)

    def __enter__(self) -> "NeptuneFetcher":
        self._client.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._client.__exit__(*args, **kwargs)

    def list_experiments(
        self,
        experiments: Union[str, ExperimentFilter, None] = None,
        limit: int = 1000,
    ) -> list[str]:
        params = {
            "pagination": {"limit": limit},
            "experimentLeader": True,
            "attributeFilters": [{"path": "sys/name"}],
        }
        query = self._get_query_string(experiments)
        if query is not None:
            params["query"] = query
        body = SearchLeaderboardEntriesParamsDTO.from_dict(params)

        result = self._client.search_entries(project_id=self._project_id, types=["run"], body=body)

        return [entry.attributes[0].string_properties.value for entry in result.entries]

    @staticmethod
    def _get_query_string(experiments: Union[str, ExperimentFilter, None]) -> Optional[str]:
        if experiments is None:
            return None
        if isinstance(experiments, str):
            return experiments
        return experiments.evaluate()
