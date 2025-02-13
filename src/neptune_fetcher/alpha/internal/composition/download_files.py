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
import os
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
from neptune_fetcher.alpha.internal.composition import (
    concurrency,
    type_inference,
)
from neptune_fetcher.alpha.internal.context import (
    Context,
    get_context,
    validate_context,
)
from neptune_fetcher.alpha.internal.retrieval import (
    attribute_definitions,
    files,
    search,
    util,
)
from neptune_fetcher.alpha.internal.retrieval.search import ContainerType


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

    if destination is None:
        destination_path = pathlib.Path.cwd()
    else:
        destination_path = pathlib.Path(destination).resolve()

    valid_context = validate_context(context or get_context())
    client = _client.get_client(valid_context)
    project = identifiers.ProjectIdentifier(valid_context.project)  # type: ignore

    _ensure_write_access(destination_path)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):
        type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project,
            filter_=experiments,
            executor=executor,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
            container_type=ContainerType.EXPERIMENT,
        )

        def process_sys_attrs_page(sys_attrs_page: util.Page[search.ExperimentSysAttrs]) -> concurrency.OUT:
            sys_id_to_label = {sys_attrs.sys_id: sys_attrs.sys_name for sys_attrs in sys_attrs_page.items}
            return _components.fetch_attribute_definition_aggregations_split(
                client=client,
                project_identifier=project,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=[sys_attrs.sys_id for sys_attrs in sys_attrs_page.items],
                downstream=lambda sys_ids_split, definitions_page, _: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=_filter_file_refs(definitions_page.items),
                    downstream=lambda values_page: concurrency.generate_concurrently(
                        items=(
                            (value.run_identifier, file_)
                            for value, file_ in zip(
                                values_page.items,
                                files.fetch_signed_urls(
                                    client=client,
                                    project_identifier=project,
                                    file_paths=[value.value for value in values_page.items],
                                ),
                            )
                        ),
                        executor=executor,
                        downstream=lambda run_file_tuple: concurrency.return_value(
                            files.download_file(
                                signed_url=run_file_tuple[1].url,
                                target_path=_create_target_path(
                                    destination=destination_path,
                                    experiment_name=sys_id_to_label[run_file_tuple[0].sys_id],
                                    attribute_path=run_file_tuple[1].path,
                                ),  # type: ignore
                            )
                        ),
                    ),
                ),
            )

        output = concurrency.generate_concurrently(
            items=search.fetch_experiment_sys_attrs(
                client=client,
                project_identifier=project,
                filter_=experiments,
            ),
            executor=executor,
            downstream=process_sys_attrs_page,
        )

        results: Generator[None, None, None] = concurrency.gather_results(output)
        list(results)


def _ensure_write_access(destination: pathlib.Path) -> None:
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    if not os.access(destination, os.W_OK):
        raise PermissionError(f"No write access to the directory: {destination}")


def _filter_file_refs(
    definitions: list[attribute_definitions.AttributeDefinition],
) -> list[attribute_definitions.AttributeDefinition]:
    return [attribute for attribute in definitions if attribute.type == "file_ref"]


def _create_target_path(
    destination: pathlib.Path, experiment_name: identifiers.SysName, attribute_path: str
) -> pathlib.Path:
    return destination / experiment_name / attribute_path
