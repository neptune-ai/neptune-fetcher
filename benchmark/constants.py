__all__ = ["FETCH_FLOAT_PROJECT", "FETCH_FLOAT_RUN_ID", "LIST_RUNS_PROJECT", "FETCH_DF_PROJECT"]

import os
from pathlib import Path

WORKSPACE = os.getenv("NEPTUNE_WORKSPACE_NAME")

FETCH_FLOAT_PROJECT = str(Path(WORKSPACE) / "fetch-floats")
FETCH_FLOAT_RUN_ID = "TES-FLOATS-FETCH-1"

LIST_RUNS_PROJECT = str(Path(WORKSPACE) / "list-runs")

FETCH_DF_PROJECT = str(Path(WORKSPACE) / "fetch-df")
