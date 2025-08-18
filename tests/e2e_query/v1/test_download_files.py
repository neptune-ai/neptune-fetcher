import os
import pathlib

import pandas as pd
import pytest

from neptune_query import (
    download_files,
    fetch_experiments_table,
    fetch_series,
)
from tests.e2e_query.data import (
    FILE_SERIES_PATHS,
    PATH,
    TEST_DATA,
)

NEPTUNE_PROJECT: str = os.getenv("NEPTUNE_E2E_PROJECT")
EXPERIMENT_NAME = TEST_DATA.experiment_names[0]


@pytest.mark.files
def test__download_files_from_table(project, temp_dir):
    # given
    attribute = f"{PATH}/files/file-value.txt"

    # when
    files = fetch_experiments_table(
        project=project.project_identifier,
        experiments=EXPERIMENT_NAME,
        attributes=attribute,
    )
    assert not files.empty
    result_df = download_files(
        files=files,
        destination=temp_dir,
    )

    # then
    expected_path = (temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-value_txt.txt").resolve()
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": None,
                f"{PATH}/files/file-value.txt": str(expected_path),
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
def test__download_files_from_file_series(project, temp_dir):
    # given
    attribute = FILE_SERIES_PATHS[0]

    # when
    file_series = fetch_series(
        project=project.project_identifier,
        experiments=EXPERIMENT_NAME,
        attributes=attribute,
    )
    assert not file_series.empty
    result_df = download_files(
        files=file_series,
        destination=temp_dir,
    )

    # then
    expected_df = pd.DataFrame(
        [
            {
                "experiment": EXPERIMENT_NAME,
                "step": step,
                f"{PATH}/files/file-series-value_0": str(
                    (
                        temp_dir / EXPERIMENT_NAME / f"{PATH}/files/file-series-value_0/step_{int(step)}_000000.bin"
                    ).resolve()
                ),
            }
            for step in [0.0, 1.0, 2.0]
        ]
    ).set_index(["experiment", "step"])
    expected_df.columns.names = ["attribute"]
    pd.testing.assert_frame_equal(result_df, expected_df)

    for step in (0.0, 1.0, 2.0):
        target_path = result_df.loc[(EXPERIMENT_NAME, step), f"{PATH}/files/file-series-value_0"]
        with open(target_path, "rb") as file:
            content = file.read()
            expected_content = f"file-0-{int(step)}".encode("utf-8")
            assert content == expected_content
