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

from http import HTTPStatus
from typing import (
    Any,
    Dict,
    Optional,
    Union,
    cast,
)

import httpx

from ... import errors
from ...client import (
    AuthenticatedClient,
    Client,
)
from ...models.client_config import ClientConfig
from ...models.error import Error
from ...types import (
    UNSET,
    Response,
    Unset,
)


def _get_kwargs(
    *,
    alpha: Union[Unset, str] = UNSET,
    x_neptune_api_token: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    headers: Dict[str, Any] = {}
    if not isinstance(x_neptune_api_token, Unset):
        headers["X-Neptune-Api-Token"] = x_neptune_api_token

    params: Dict[str, Any] = {}

    params["alpha"] = alpha

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/api/backend/v1/clients/config",
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, ClientConfig, Error]]:
    try:
        if response.status_code == HTTPStatus.OK:
            response_200 = ClientConfig.from_dict(response.json())

            return response_200
        if response.status_code == HTTPStatus.BAD_REQUEST:
            response_400 = Error.from_dict(response.json())

            return response_400
        if response.status_code == HTTPStatus.UNAUTHORIZED:
            response_401 = cast(Any, None)
            return response_401
        if response.status_code == HTTPStatus.FORBIDDEN:
            response_403 = cast(Any, None)
            return response_403
        if response.status_code == HTTPStatus.NOT_FOUND:
            response_404 = cast(Any, None)
            return response_404
        if response.status_code == HTTPStatus.REQUEST_TIMEOUT:
            response_408 = cast(Any, None)
            return response_408
        if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
            response_422 = cast(Any, None)
            return response_422
    except Exception as e:
        raise errors.UnableToParseResponse(e, response) from e

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Any, ClientConfig, Error]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    alpha: Union[Unset, str] = UNSET,
    x_neptune_api_token: Union[Unset, str] = UNSET,
) -> Response[Union[Any, ClientConfig, Error]]:
    """
    Args:
        alpha (Union[Unset, str]):
        x_neptune_api_token (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ClientConfig, Error]]
    """

    kwargs = _get_kwargs(
        alpha=alpha,
        x_neptune_api_token=x_neptune_api_token,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    alpha: Union[Unset, str] = UNSET,
    x_neptune_api_token: Union[Unset, str] = UNSET,
) -> Optional[Union[Any, ClientConfig, Error]]:
    """
    Args:
        alpha (Union[Unset, str]):
        x_neptune_api_token (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ClientConfig, Error]
    """

    return sync_detailed(
        client=client,
        alpha=alpha,
        x_neptune_api_token=x_neptune_api_token,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    alpha: Union[Unset, str] = UNSET,
    x_neptune_api_token: Union[Unset, str] = UNSET,
) -> Response[Union[Any, ClientConfig, Error]]:
    """
    Args:
        alpha (Union[Unset, str]):
        x_neptune_api_token (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ClientConfig, Error]]
    """

    kwargs = _get_kwargs(
        alpha=alpha,
        x_neptune_api_token=x_neptune_api_token,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    alpha: Union[Unset, str] = UNSET,
    x_neptune_api_token: Union[Unset, str] = UNSET,
) -> Optional[Union[Any, ClientConfig, Error]]:
    """
    Args:
        alpha (Union[Unset, str]):
        x_neptune_api_token (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ClientConfig, Error]
    """

    return (
        await asyncio_detailed(
            client=client,
            alpha=alpha,
            x_neptune_api_token=x_neptune_api_token,
        )
    ).parsed
