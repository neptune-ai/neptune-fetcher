import os
import pathlib

import pandas as pd
import pytest

from neptune_query import (
    fetch_experiments_table,
    fetch_series,
)
from neptune_query._internal import resolve_files
from neptune_query.filters import AttributeFilter
from neptune_query.internal.composition.download_files import download_files
from neptune_query.internal.retrieval.search import ContainerType
from neptune_query.types import File
from tests.e2e_query.data import (
    FILE_SERIES_PATHS,
    PATH,
    TEST_DATA,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = TEST_DATA.experiment_names[0]


@pytest.mark.files
def test_download_files_missing(client, project, experiment_identifier, temp_dir):
    # when
    result_df = download_files(
        files=[
            File(
                project_identifier=project.project_identifier,
                experiment_name=EXPERIMENT_NAME,
                run_id=None,
                attribute_path=f"{PATH}/files/object-does-not-exist",
                step=None,
                path="object-does-not-exist",
                size_bytes=0,
                mime_type="application/octet-stream",
            ),
        ],
        destination=temp_dir,
        container_type=ContainerType.EXPERIMENT,
        context=None,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": None,
                f"{PATH}/files/object-does-not-exist": None,
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.files
def test_download_files_no_permission(client, project, experiment_identifier, temp_dir):
    os.chmod(temp_dir, 0o000)  # No permissions

    with pytest.raises(PermissionError):
        download_files(
            files=[
                File(
                    project_identifier=project.project_identifier,
                    experiment_name=EXPERIMENT_NAME,
                    run_id=None,
                    attribute_path=f"{PATH}/files/file-value.txt",
                    step=None,
                    path="not-real-path",
                    size_bytes=14,
                    mime_type="text/plain",
                ),
            ],
            destination=temp_dir,
            container_type=ContainerType.EXPERIMENT,
            context=None,
        )

    os.chmod(temp_dir, 0o755)  # Reset permissions


@pytest.mark.files
def test_download_files_destination_file_type(client, project, experiment_identifier, temp_dir):
    destination = temp_dir / "file"
    with open(destination, "wb") as file:
        file.write(b"test")

    with pytest.raises(NotADirectoryError):
        download_files(
            files=[
                File(
                    project_identifier=project.project_identifier,
                    experiment_name=EXPERIMENT_NAME,
                    run_id=None,
                    attribute_path=f"{PATH}/files/file-value.txt",
                    step=None,
                    path="not-a-real-path",
                    size_bytes=14,
                    mime_type="text/plain",
                ),
            ],
            destination=destination,
            container_type=ContainerType.EXPERIMENT,
            context=None,
        )


@pytest.mark.files
def test_download_files_single(client, project, experiment_identifier, temp_dir):
    # when
    files_df = fetch_experiments_table(
        experiments=EXPERIMENT_NAME,
        attributes=AttributeFilter(name=f"{PATH}/files/file-value.txt", type="file"),
        project=project.project_identifier,
    )
    downloadable_files = [files_df.loc[EXPERIMENT_NAME, f"{PATH}/files/file-value.txt"]]
    result_df = download_files(
        files=downloadable_files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": None,
                f"{PATH}/files/file-value.txt": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt.txt"),
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    target_path = result_df.loc[(EXPERIMENT_NAME, None), f"{PATH}/files/file-value.txt"]
    assert pathlib.Path(target_path).exists()
    with open(target_path, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_files_multiple(client, project, experiment_identifier, temp_dir):
    # when
    files_df = fetch_experiments_table(
        experiments=EXPERIMENT_NAME,
        attributes=AttributeFilter(name=[f"{PATH}/files/file-value", f"{PATH}/files/file-value.txt"], type="file"),
        project=project.project_identifier,
    )
    downloadable_files = [
        files_df.loc[EXPERIMENT_NAME, f"{PATH}/files/file-value"],
        files_df.loc[EXPERIMENT_NAME, f"{PATH}/files/file-value.txt"],
    ]
    result_df = download_files(
        files=downloadable_files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": None,
                f"{PATH}/files/file-value": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value.bin"),
                f"{PATH}/files/file-value.txt": str(temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt.txt"),
            }
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    target_path_value = result_df.loc[(EXPERIMENT_NAME, None), f"{PATH}/files/file-value"]
    with open(target_path_value, "rb") as file:
        content = file.read()
        assert content == b"Binary content"
    target_path_value_txt = result_df.loc[(EXPERIMENT_NAME, None), f"{PATH}/files/file-value.txt"]
    with open(target_path_value_txt, "rb") as file:
        content = file.read()
        assert content == b"Text content"


@pytest.mark.files
def test_download_file_series(client, project, experiment_identifier, temp_dir):
    # when
    files_df = fetch_series(
        experiments=EXPERIMENT_NAME,
        attributes=AttributeFilter(name=FILE_SERIES_PATHS, type="file_series"),
        project=project.project_identifier,
    )
    files = resolve_files(files_df)
    assert len(files) == 6
    result_df = download_files(files=files, destination=temp_dir, container_type=ContainerType.EXPERIMENT, context=None)

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": step,
                f"{PATH}/files/file-series-value_0": str(
                    temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-series-value_0/step_{int(step)}_000000.bin"
                ),
                f"{PATH}/files/file-series-value_1": str(
                    temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-series-value_1/step_{int(step)}_000000.bin"
                ),
            }
            for step in [0.0, 1.0, 2.0]
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    for attribute in (0, 1):
        for step in (0.0, 1.0, 2.0):
            target_path = result_df.loc[(EXPERIMENT_NAME, step), f"{PATH}/files/file-series-value_{attribute}"]
            with open(target_path, "rb") as file:
                content = file.read()
                expected_content = f"file-0-{int(step)}".encode("utf-8")
                assert content == expected_content
