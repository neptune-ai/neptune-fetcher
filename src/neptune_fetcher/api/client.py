from typing import Any, List

from neptune_retrieval_api.models import (
    SearchLeaderboardEntriesParamsDTO,
)

from ..filter import ExperimentFilter
from ..context import Context
from .auth import create_authenticated_client
from .backend import NeptuneApiProtoBackend


class Client:
    def __init__(self, project_id: str, backend: NeptuneApiProtoBackend):
        self._project_id = project_id
        self._backend = backend

    @staticmethod
    def create(context: Context) -> "Client":
        auth_client = create_authenticated_client(api_token=context.api_token, proxies=context.proxies)
        backend = NeptuneApiProtoBackend(auth_client=auth_client)
        return Client(project_id=context.project, backend=backend)

    def __enter__(self) -> "Client":
        self._backend.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._backend.__exit__(*args, **kwargs)

    def list_experiments(
            self,
            experiments: str | ExperimentFilter | None = None,
            limit: int = 1000,
    ) -> List[str]:
        if isinstance(experiments, str):
            experiments = ExperimentFilter.name_in(experiments)
        if experiments is None:
            experiments = ExperimentFilter()
        query = experiments.to_query()

        body = SearchLeaderboardEntriesParamsDTO.from_dict(
            {
                "attributeFilters": [{"path": name} for name in columns],
                "pagination": {"limit": limit},
                "experimentLeader": True,
                "query": {"query": query},
            }
        )

        result = self._backend.search_entries(
            project_id=self._project_id,
            types=["run"],
            body=body
        )

        return [entry.experiment_id for entry in result.entries]
