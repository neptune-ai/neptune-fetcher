#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
#
from neptune_fetcher import (
    ReadOnlyProject,
    ReadOnlyRun,
)

PROJECT = "<PROJECT HERE>"


def main():
    project = ReadOnlyProject(project=PROJECT)

    run_info = list(project.list_runs())
    print("Run info list:\n", run_info[:10], "\n###########################################\n")

    run = ReadOnlyRun(read_only_project=project, with_id="TES-1")

    # Fetch single attribute
    lr = run["params/lr"].fetch()
    print("Learning rate:\n", lr, "\n###########################################\n")

    # Fetch series
    series = run["series/my_float_series"].fetch_values()
    print("Float series:\n", series, "\n###########################################\n")

    # Fetch runs table
    run_df = project.fetch_runs_df(
        columns=["sys/id", "sys/name", "sys/owner"],
        with_ids=[run["sys/id"] for run in run_info[:10]],
    )
    print("Runs dataframe:\n", run_df, "\n###########################################\n")

    # Run attributes
    attributes = list(run.field_names)
    print("Run attribute names:\n", attributes, "\n###########################################\n")


if __name__ == "__main__":
    main()
