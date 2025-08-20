import os
import sys

import httpx
from neptune_api.credentials import Credentials

from neptune_fetcher.internal.api_utils import (
    create_auth_api_client,
    get_config_and_token_urls,
)


def create_project(client: httpx.Client, organization, name):
    body = {"organizationIdentifier": organization, "name": name, "visibility": "priv"}
    args = {
        "method": "post",
        "url": "/api/backend/v1/projects",
        "json": body,
    }

    response = client.request(**args)
    response.raise_for_status()


def delete_project(client: httpx.Client, organization, name):
    project_identifier = f"{organization}/{name}"
    args = {
        "method": "delete",
        "url": "/api/backend/v1/projects",
        "params": {"projectIdentifier": project_identifier},
    }
    response = client.request(**args)
    response.raise_for_status()


if __name__ == "__main__":
    api_token = os.getenv("NEPTUNE_API_TOKEN")
    if api_token is None:
        raise ValueError("NEPTUNE_API_TOKEN not set")

    credentials = Credentials.from_api_key(api_key=api_token)
    config, token_urls = get_config_and_token_urls(credentials=credentials)
    api_client = create_auth_api_client(credentials=credentials, config=config, token_refreshing_urls=token_urls)
    httpx_client = api_client.get_httpx_client()

    cmd = sys.argv[1]
    if cmd == "create_project":
        create_project(httpx_client, *sys.argv[2:4])
    elif cmd == "delete_project":
        delete_project(httpx_client, *sys.argv[2:4])
    else:
        print("Unknown command")
        sys.exit(1)
