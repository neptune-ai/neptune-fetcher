import sys

from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.internal.credentials import Credentials


def create_project(backend, organization, name):
    backend.api.createProject(
        projectToCreate=dict(organizationIdentifier=organization, name=name, visibility="workspace")
    )


def delete_project(backend, organization, name):
    backend.api.deleteProject(projectIdentifier="/".join([organization, name]))


if __name__ == "__main__":
    credentials = Credentials.from_token()
    backend = HostedNeptuneBackend(credentials=credentials).backend_client

    cmd = sys.argv[1]
    if cmd == "create_project":
        create_project(backend, *sys.argv[2:4])
    elif cmd == "delete_project":
        delete_project(backend, *sys.argv[2:4])
    else:
        print("Unknown command")
        sys.exit(1)
