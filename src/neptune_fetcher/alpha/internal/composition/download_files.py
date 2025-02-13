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
    Union,
)

from neptune_fetcher.alpha.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from neptune_fetcher.alpha.internal import client as _client
from neptune_fetcher.alpha.internal import identifiers
from neptune_fetcher.alpha.internal.composition import attribute_components as _components
from neptune_fetcher.alpha.internal.composition import concurrency
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.retrieval import (
    files,
    search,
)


def download_files(
    experiments: Optional[Union[str, Filter]] = None,
    attributes: Optional[Union[str, AttributeFilter]] = None,
    destination: Optional[str] = None,
    context: Optional[Context] = None,
) -> None:
    """
    Downloads files associated with selected experiments and attributes.

    Args:
      experiments: Specifies the experiment(s) to filter files by.
          - A string representing the experiment name or a `Filter` object for more complex filtering.
          - If `None`, all experiments are considered.

      attributes: Specifies the attribute(s) to filter files by within the selected experiments.
          - A string representing the attribute path or an `AttributeFilter` object.
          - If `None`, all attributes are considered.

      destination: The directory where files will be downloaded.
          - If `None`, the current working directory (CWD) is used as the default.
          - The path can be relative or absolute.

      context: Provides additional contextual information for the download (optional).
          - A `Context` object, which may include things like credentials or other metadata.

    Download Path Construction:
      - Files are downloaded to the following directory structure:
          <destination>/<experiment_name>/<attribute_path>/<file_name>
      - If `<experiment_name>` or `<attribute_path>` contains '/', corresponding subdirectories will be created.
      - The `<file_name>` is the final part of the file's path on object storage after splitting it by '/'.

    Example:
      Given an experiment named "some/experiment" and an attribute "some/attribute" with an uploaded file path
      of "/my/path/on/object/storage/file.txt":

          download_files(experiments="some/experiment", attributes="some/attribute", destination="/my/destination")

      The file will be downloaded to:

          /my/destination/some/experiment/some/attribute/file.txt

    Notes:
      - If the experiment or attribute paths include slashes ('/'), they will be treated as subdirectory structures,
        and those directories will be created during the download process.
      - Ensure that the `destination` directory has write permissions for successful file downloads.
      - If the specified destination or any subdirectories do not exist, they will be automatically created.
    """
    pass
    if isinstance(experiments, str):
        experiments = Filter.matches_all(Attribute("sys/name", type="string"), experiments)

    if isinstance(attributes, str):
        attributes = AttributeFilter(name_matches_all=attributes, type_in=["file_ref"])
    elif attributes is None:
        attributes = AttributeFilter(type_in=["file_ref"])

    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    destination_path = pathlib.Path(destination or ".").resolve()
    print(destination_path)
    # destination_path.mkdir(parents=True, exist_ok=True)
    # TODO: check write permission before starting download

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        # TODO: type inference. special case for file_ref?

        output = concurrency.generate_concurrently(
            items=search.fetch_experiment_sys_ids(
                client=client,
                project_identifier=project,
                filter_=experiments,
            ),
            executor=executor,
            downstream=lambda sys_ids_page: _components.fetch_attribute_definition_aggregations_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids_page.items,
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=definitions_page.items,
                    downstream=lambda values_page: concurrency.generate_concurrently(
                        items=(
                            f
                            for f in files.fetch_signed_urls(
                                client=client,
                                project_identifier=project,
                                file_paths=[value.value for value in values_page.items],
                            )
                        ),
                        executor=executor,
                        downstream=concurrency.return_value,
                    ),
                ),
            ),
        )

        results: Generator[files.SignedFile, None, None] = concurrency.gather_results(output)
        print(list(results))  # TODO ofc
