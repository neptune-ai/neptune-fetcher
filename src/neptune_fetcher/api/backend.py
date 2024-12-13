from typing import Any, Callable, TypeVar
import time

import neptune_api.client
from neptune_retrieval_api.api.default import search_leaderboard_entries_proto
from neptune_retrieval_api.models import (
    SearchLeaderboardEntriesParamsDTO,
)
from neptune_retrieval_api.proto.neptune_pb.api.model.leaderboard_entries_pb2 import (
    ProtoLeaderboardEntriesSearchResultDTO,
)
from neptune_retrieval_api.types import Response

from ..errors import NeptuneException

T = TypeVar("T")


class NeptuneApiProtoBackend:
    def __init__(
        self,
        auth_client: neptune_api.client.AuthenticatedClient
    ):
        self._auth_client = auth_client

    def __enter__(self) -> "NeptuneApiProtoBackend":
        self._auth_client.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._auth_client.__exit__(*args, **kwargs)

    def search_entries(
            self,
            project_id: str,
            types: list[str],
            body: SearchLeaderboardEntriesParamsDTO
    ) -> ProtoLeaderboardEntriesSearchResultDTO:
        response = self._backoff_retry(
            lambda: search_leaderboard_entries_proto.sync_detailed(
                client=self._auth_client, project_identifier=project_id, type=types, body=body
            )
        )
        result = ProtoLeaderboardEntriesSearchResultDTO.FromString(response.content)
        return result

    @staticmethod
    def _backoff_retry(
            func: Callable[[], Response[T]], *args, max_tries: int = 5, backoff_factor: float = 0.5, max_backoff: float = 30.0, **kwargs
    ) -> Response[T]:
        """
        Retries a function with exponential backoff. The function will be called at most `max_tries` times.

        :param func: The function to retry.
        :param max_tries: Maximum number of times `func` will be called, including retries.
        :param backoff_factor: Factor by which the backoff time increases.
        :param max_backoff: Maximum backoff time.
        :param args: Positional arguments to pass to the function.
        :param kwargs: Keyword arguments to pass to the function.
        :return: The result of the function call.
        """

        if max_tries < 1:
            raise ValueError("max_tries must be greater than or equal to 1")

        tries = 0
        last_exc = None
        last_response = None

        while True:
            tries += 1
            try:
                response = func(*args, **kwargs)
            except Exception as e:
                response = None
                last_exc = e

            if response is not None:
                last_response = response

                code = response.status_code.value
                if 0 <= code < 300:
                    return response

                # Not a TooManyRequests or InternalServerError code
                if not (code == 429 or 500 <= code < 600):
                    raise NeptuneException(f"Unexpected server response {response.status_code}: {str(response.content)}")

            if tries == max_tries:
                break

            # A retryable error occurred, back off and try again
            backoff_time = min(backoff_factor * (2**tries), max_backoff)
            time.sleep(backoff_time)

        # No more retries left
        msg = []
        if last_exc:
            msg.append(f"Last exception: {str(last_exc)}")
        if last_response:
            msg.append(f"Last response: {last_response.status_code}: {str(last_response.content)}")
        if not msg:
            raise NeptuneException("Unknown error occurred when requesting data")

        raise NeptuneException(f"Failed to get response after {tries} retries. " + "\n".join(msg))

