import os
import pathlib
import tempfile

import pandas as pd
import pytest

from neptune_fetcher.internal.composition.download_files import download_files
from neptune_fetcher.internal.filters import (
    AttributeFilter,
    Filter,
)
from neptune_fetcher.internal.retrieval.search import ContainerType
from tests.e2e.data import (
    PATH,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = TEST_DATA.experiment_names[0]


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


def test_download_files_missing(client, project, experiment_identifier, temp_dir):
    # when
    result_df = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=[f"{PATH}/files/object-does-not-exist"]),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                f"{PATH}/files/object-does-not-exist": None,
            }
        ]
    ).set_index("experiment")
    expected_df.columns.names = ["attribute"]
    assert result_df.equals(expected_df)


def test_download_files_no_permission(client, project, experiment_identifier, temp_dir):
    os.chmod(temp_dir, 0o000)  # No permissions

    with pytest.raises(PermissionError):
        download_files(
            filter_=Filter.name_in(EXPERIMENT_NAME),
            attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value.txt"]),
            destination=temp_dir,
            context=None,
            container_type=ContainerType.EXPERIMENT,
        )

    os.chmod(temp_dir, 0o755)  # Reset permissions


def test_download_files_single(client, project, experiment_identifier, temp_dir):
    # when
    result_df = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value.txt"]),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                f"{PATH}/files/file-value.txt": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt"),
            }
        ]
    ).set_index("experiment")
    expected_df.columns.names = ["attribute"]
    assert result_df.equals(expected_df)

    target_path = result_df.loc[EXPERIMENT_NAME, f"{PATH}/files/file-value.txt"]
    assert pathlib.Path(target_path).exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


def test_download_files_multiple(client, project, experiment_identifier, temp_dir):
    # when
    result_df = download_files(
        filter_=Filter.name_in(EXPERIMENT_NAME),
        attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value", f"{PATH}/files/file-value.txt"]),
        destination=temp_dir,
        context=None,
        container_type=ContainerType.EXPERIMENT,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                f"{PATH}/files/file-value": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value"),
                f"{PATH}/files/file-value.txt": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt"),
            }
        ]
    ).set_index("experiment")
    expected_df.columns.names = ["attribute"]
    assert result_df.equals(expected_df)

    for row in result_df.iterrows():
        for attr, path_value in row[1].items():
            target_path = pathlib.Path(path_value)
            assert target_path.exists()
            with open(target_path, "rb") as file:
                content = file.read()
                if attr == f"{PATH}/files/file-value":
                    assert content == b"Binary content"
                else:
                    assert content == b"Text content"


def test_download_files_destination_a_file(client, project, experiment_identifier, temp_dir):
    destination = temp_dir / "file"
    with open(destination, "wb") as file:
        file.write(b"test")

    with pytest.raises(NotADirectoryError):
        download_files(
            filter_=Filter.name_in(EXPERIMENT_NAME),
            attributes=AttributeFilter(name_eq=[f"{PATH}/files/file-value.txt"]),
            destination=destination,
            context=None,
            container_type=ContainerType.EXPERIMENT,
        )
