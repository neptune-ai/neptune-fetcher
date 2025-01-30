import os
from pathlib import Path
from typing import (
    Callable,
    Final,
    Optional,
)

import httpx
from tqdm import tqdm

from neptune_fetcher.api.api_client import ApiClient

NEPTUNE_VERIFY_SSL: Final[bool] = os.environ.get("NEPTUNE_VERIFY_SSL", "1").lower() in {"1", "true"}


def download_file(
    backend: ApiClient,
    *,
    project_name: str,
    attribute_path: str,
    file_path: str,
    destination: Optional[str] = None,
    overwrite: bool = False,
    experiment_name: Optional[str] = None,
    resolve_destination_fn: Callable[[str, Optional[str], Optional[str], Optional[str]], Path],
):
    """
    resolve_destination_fn: callable that takes user-provided arguments:
        filename, destination, attribute_path, experiment_name
        and returns a Path object pointing to the target local download path.
    """
    # TODO: Catch all exceptions and convert to ours? Probably doesn't make sense in all cases, but maybe in some:
    #   - HTTP response code != 200, 404 and 403 in particular
    # TODO: do backoff and resume on failure

    dest_path = resolve_destination_fn(file_path, destination, attribute_path, experiment_name)
    if dest_path.is_file() and not overwrite:
        raise FileExistsError(f"File already exists: `{dest_path}`. Pass overwrite=True to overwrite it.")

    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        raise FileExistsError(f"Destination path `{dest_path.parent}` is a file, not a directory.")

    with open(dest_path, "wb") as file, tqdm(total=0, unit="B", unit_scale=True, unit_divisor=1024) as progress:
        progress.set_description("Requesting file download URL")
        url = backend.fetch_file_download_url(project_name=project_name, file_path=file_path, permission="read")

        progress.set_description(f"Downloading file {dest_path}")
        with httpx.Client(verify=NEPTUNE_VERIFY_SSL) as client:
            response = client.get(url)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            progress.total = total_size
            progress.refresh()

            for chunk in response.iter_bytes(chunk_size=1024):
                file.write(chunk)
                progress.update(len(chunk))
