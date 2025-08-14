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
import pathlib
from typing import (
    Generator,
    Optional,
)

import pandas as pd

from ... import types
from .. import client as _client
from ..composition import (
    concurrency,
    validation,
)
from ..context import (
    Context,
    get_context,
    validate_context,
)
from ..output_format import create_files_dataframe
from ..retrieval import files as _files
from ..retrieval.search import ContainerType
from ..retrieval.split import split_files


def download_files(
    *,
    files: list[types.File],
    destination: pathlib.Path,
    container_type: ContainerType,
    context: Optional[Context] = None,
) -> pd.DataFrame:
    validation.ensure_write_access(destination)
    valid_context = validate_context(context or get_context())
    client = _client.get_client(context=valid_context)

    with concurrency.create_thread_pool_executor() as executor:

        def generate_signed_files() -> Generator[tuple[types.File, _files.SignedFile], None, None]:
            for file_group in split_files(files):
                signed_files = _files.fetch_signed_urls(client=client, files=file_group)
                yield from zip(file_group, signed_files)

        output = concurrency.generate_concurrently(
            items=generate_signed_files(),
            executor=executor,
            downstream=lambda file_tuple: concurrency.return_value(
                (
                    file_tuple[0],
                    _files.download_file_complete(
                        client=client,
                        signed_file=file_tuple[1],
                        target_path=_files.create_target_path(
                            destination=destination,
                            file=file_tuple[0],
                        ),
                    ),
                )
            ),
        )

        results: Generator[
            tuple[types.File, Optional[pathlib.Path]],
            None,
            None,
        ] = concurrency.gather_results(output)

        file_paths: dict[types.File, Optional[pathlib.Path]] = {}
        for file, path in results:
            file_paths[file] = path

        return create_files_dataframe(
            file_paths,
            container_type=container_type,
        )
