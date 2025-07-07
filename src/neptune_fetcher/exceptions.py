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

import platform
import warnings
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Sequence,
)

import neptune_fetcher.internal.env as env


class NeptuneError(Exception):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message.format(**kwargs, **_styles))


class NeptuneWarning(Warning):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message.format(**kwargs, **_styles))


class NeptuneUserError(NeptuneError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)


class NeptuneProjectNotProvided(NeptuneUserError):
    def __init__(self) -> None:
        super().__init__(
            """
{h1}NeptuneProjectNotProvided: The project name was not provided.{end}

Make sure to specify a valid project in one of the following ways:

- Call the `set_project()` function
- Create a Context with the project name and pass it to the `context` argument of the fetching method
- Set the `NEPTUNE_PROJECT` environment variable

For details, see https://docs.neptune.ai/fetcher_setup
"""
        )


class NeptuneFailedToFetchClientConfig(NeptuneError):
    def __init__(self, exception: Exception) -> None:
        super().__init__(
            """
{h1}NeptuneFailedToFetchClientConfigException: Failed to fetch the client configuration.{end}

This can happen due to network issues or if Neptune is unreachable.

Make sure that you have a stable internet connection and that your Neptune instance is running and accessible.

If the issue persists, please contact Neptune support with the following information:

{exception}
""",
            exception=exception,
        )


class NeptuneProjectInaccessible(NeptuneError):
    def __init__(self) -> None:
        super().__init__(
            """
{h1}NeptuneProjectInaccessible: You don't have access to the project or it doesn't exist.{end}

Ensure that:
- the workspace and project names are correct
- the account you're using has at least Viewer access to the project

For details, see https://docs.neptune.ai/project_access
"""
        )


class NeptuneApiTokenNotProvided(NeptuneUserError):
    def __init__(self) -> None:
        super().__init__(
            """
{h1}NeptuneApiTokenNotProvided: The Neptune API token was not provided.{end}

Make sure to specify a valid token in one of the following ways:

- Call the `set_api_token()` function
- Create a Context with the API token and pass it to the `context` argument of the fetching method
- Set the `NEPTUNE_API_TOKEN` environment variable

For details, see https://docs.neptune.ai/fetcher_setup
"""
        )


class NeptuneInvalidCredentialsError(NeptuneUserError):
    def __init__(self) -> None:
        super().__init__(
            """
{h1}NeptuneInvalidCredentialsError: Your Neptune API token was rejected by the server.{end}

The API token must be valid and the associated account must have access to the target project.
Make sure to specify your Neptune credentials in one of the following ways:

- Call the `set_api_token()` or `set_project()` functions
- Create a Context with the API token and project, then pass it to the `context` argument of the fetching method
- Set the `NEPTUNE_API_TOKEN` and `NEPTUNE_PROJECT` environment variables

For details, see:
https://docs.neptune.ai/fetcher_setup
https://docs.neptune.ai/project_access
"""
        )


class AttributeTypeInferenceError(NeptuneError):
    def __init__(self, attribute_names: Sequence[str], details: Optional[Iterable[str]] = None) -> None:
        attribute_str = (
            f"attribute {attribute_names[0]}"
            if len(attribute_names) == 1
            else f"attributes [{', '.join(attribute_names)}]"
        )

        super().__init__(
            """
{h1}AttributeTypeInferenceError: Failed to infer types for {attribute_names}{end}
{details}
To resolve this ambiguity, specify the attribute type explicitly when constructing a filter:
    {python}
    fetch_experiments_table(
        experiments=Filter.eq(Attribute("metrics/m1", aggregation="max", type="float_series"), 1.2),
        ...
        sort_by=Attribute("config/batch_size", type="int"),
        ...
    )

    fetch_metrics(
        experiments=Filter.eq(Attribute("config/batch_size", type="int"), 64)
                    | Filter.eq(Attribute("config/batch_size", type="float"), 64.0),
        ...
    )

    list_attributes(
        experiments=Filter.eq(Attribute("config/batch_size", type="int"), 64),
        ...
    )
    {end}

For details, see https://docs.neptune.ai/attribute_types
""",
            attribute_names=attribute_str,
            details="\n" + "\n".join(details) if details else "",
        )


class ConflictingAttributeTypes(NeptuneError):
    def __init__(self, attribute_names: Iterable[str]) -> None:
        super().__init__(
            """
{h1}ConflictingAttributeTypes: Multiple types detected for attributes [{attribute_names}]{end}

Neptune found the attribute name in multiple runs, but the attribute type is not the same across all runs.

- To present each type as a separate column, use {python}type_suffix_in_column_names=True{end}.
- Alternatively, specify the attribute type explicitly using {python}AttributeFilter(..., type_in=[...]){end}.

For details, see https://docs.neptune.ai/attribute_types
""",
            attribute_names=", ".join(attribute_names),
        )


class NeptuneUnexpectedResponseError(NeptuneError):
    def __init__(self, status_code: int, content: bytes) -> None:
        super().__init__(
            """
{h1}NeptuneUnexpectedResponseError: The Neptune server returned an unexpected response.{end}

Response status: {status_code}
Response content: {content}
""",
            status_code=status_code,
            content=_decode_content(content),
        )


class NeptuneRetryError(NeptuneError):
    def __init__(
        self, retries: int, time: float, last_status_code: Optional[int] = None, last_content: Optional[bytes] = None
    ) -> None:
        content_str = _decode_content(last_content) if last_content else ""
        super().__init__(
            """
{h1}NeptuneRetryError: The Neptune server returned an error after {retries} retries, {time:.2f} seconds.{end}

{status_code_line}
{content_line}
""",
            retries=retries,
            time=time,
            status_code_line=f"Last response status: {last_status_code}" if last_status_code is not None else "",
            content_line=f"Last response content: {content_str}" if last_content is not None else "",
        )


def _decode_content(content: bytes, content_max_length: int = 1000) -> str:
    try:
        return content.decode("utf-8")[:content_max_length]
    except UnicodeDecodeError:
        return repr(content)[:content_max_length]


EMPTY_STYLES = {
    "h1": "",
    "h2": "",
    "blue": "",
    "python": "",
    "bash": "",
    "warning": "",
    "correct": "",
    "fail": "",
    "bold": "",
    "underline": "",
    "end": "",
}

UNIX_STYLES = {
    "h1": "\033[95m",
    "h2": "\033[94m",
    "blue": "\033[94m",
    "python": "\033[96m",
    "bash": "\033[95m",
    "warning": "\033[93m",
    "correct": "\033[92m",
    "fail": "\033[91m",
    "bold": "\033[1m",
    "underline": "\033[4m",
    "end": "\033[0m",
}

_styles = UNIX_STYLES if platform.system() in ["Linux", "Darwin"] else EMPTY_STYLES


def _get_styles() -> Dict[str, str]:  # TODO: unused?
    if env.NEPTUNE_ENABLE_COLORS.get():
        return _styles
    return EMPTY_STYLES


warnings.simplefilter("once", category=NeptuneWarning)

# We keep a set of types we've warned the user about to make sure we warn about a type only once.
# This is necessary because of a bug in pandas, that causes duplicate warnings to be issued everytime after an
# DataFrame() is created (presumably only empty DF).
# The bug basically makes `warnings.simplefilter("once", NeptuneWarning)` not work as expected, and would flood
# the user with warnings in some cases.
_warned_types = set()


def warn_unsupported_value_type(type_: str) -> None:
    if type_ in _warned_types:
        return

    _warned_types.add(type_)
    warnings.warn(
        f"A value of type `{type_}` was returned by your query. This type is not supported by your installed version "
        "of neptune-fetcher. Values will evaluate to `None` and empty DataFrames. "
        "Upgrade neptune-fetcher to access this data.",
        NeptuneWarning,
    )
