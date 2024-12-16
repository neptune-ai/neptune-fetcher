from typing import Any

from neptune_retrieval_api.models import (
    SearchLeaderboardEntriesParamsDTO,
)

from ..filter import ExperimentFilter
from ..context import Context
from .auth import create_authenticated_client
from .client import ProtoNeptuneApiClient


class NeptuneFetcher:
    def __init__(self, project_id: str, client: ProtoNeptuneApiClient):
        self._project_id = project_id
        self._client = client

    @staticmethod
    def create(context: Context) -> "NeptuneFetcher":
        auth_client = create_authenticated_client(api_token=context.api_token, proxies=context.proxies)
        client = ProtoNeptuneApiClient(auth_client=auth_client)
        return NeptuneFetcher(project_id=context.project, client=client)

    def __enter__(self) -> "NeptuneFetcher":
        self._client.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._client.__exit__(*args, **kwargs)

    def list_experiments(
            self,
            experiments: str | ExperimentFilter | None = None,
            limit: int = 1000,
    ) -> list[str]:
        if experiments is None:
            query = None
        else:
            if isinstance(experiments, str):
                experiments = ExperimentFilter.name_in(experiments)
            query = {"query": experiments.to_query()}

        body = SearchLeaderboardEntriesParamsDTO.from_dict(
            {
                "pagination": {"limit": limit},
                "experimentLeader": True,
                "query": query
            }
        )

        result = self._client.search_entries(
            project_id=self._project_id,
            types=["run"],
            body=body
        )

        return [entry.experiment_id for entry in result.entries]
