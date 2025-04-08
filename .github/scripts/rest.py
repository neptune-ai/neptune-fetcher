import os
import sys

from neptune_fetcher.api.api_client import ApiClient


def create_project(backend: ApiClient, organization, name):
    body = {"organizationIdentifier": organization, "name": name, "visibility": "priv"}
    args = {
        "method": "post",
        "url": "/api/backend/v1/projects",
        "json": body,
    }

    response = backend._backend.get_httpx_client().request(**args)
    response.raise_for_status()


def delete_project(backend, organization, name):
    project_identifier = f"{organization}/{name}"
    args = {
        "method": "delete",
        "url": "/api/backend/v1/projects",
        "params": {"projectIdentifier": project_identifier},
    }
    response = backend._backend.get_httpx_client().request(**args)
    response.raise_for_status()


if __name__ == "__main__":
    api_token = os.getenv("NEPTUNE_API_TOKEN")
    if api_token is None:
        raise ValueError("NEPTUNE_API_TOKEN not set")

    backend = ApiClient(api_token=api_token)

    cmd = sys.argv[1]
    if cmd == "create_project":
        create_project(backend, *sys.argv[2:4])
    elif cmd == "delete_project":
        delete_project(backend, *sys.argv[2:4])
    else:
        print("Unknown command")
        sys.exit(1)
