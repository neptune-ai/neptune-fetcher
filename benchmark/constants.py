__all__ = ["FETCH_FLOAT_PROJECT", "FETCH_FLOAT_RUN_ID", "LIST_RUNS_PROJECT", "FETCH_DF_PROJECT"]

from pathlib import Path

WORKSPACE = "<workspace-name>"

FETCH_FLOAT_PROJECT = str(Path(WORKSPACE) / "fetch-floats")
FETCH_FLOAT_RUN_ID = "TES-FLOATS-FETCH-1"

LIST_RUNS_PROJECT = str(Path(WORKSPACE) / "list-runs")

FETCH_DF_PROJECT = str(Path(WORKSPACE) / "fetch-df")
