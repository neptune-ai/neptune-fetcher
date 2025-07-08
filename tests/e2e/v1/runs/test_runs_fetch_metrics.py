import os

import pandas as pd
import pytest

import neptune_query.runs as runs
from neptune_fetcher.internal import identifiers
from neptune_fetcher.internal.output_format import create_metrics_dataframe
from tests.e2e.v1.generator import (
    RUN_BY_ID,
    timestamp_for_step,
)

NEPTUNE_PROJECT = os.getenv("NEPTUNE_E2E_PROJECT")


@pytest.mark.parametrize(
    "runs_filter, attributes_filter, expected_metrics, tail_limit, step_range, lineage_to_the_root",
    [
        (
            r"^non_existent_run_name$",
            r"^foo0$",
            {},
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^non_existent_attribute_name$",
            {},
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {
                ("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0"),
            },
            None,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[-3:]},
            3,
            (None, None),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[0:7]},
            None,
            (0, 6),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[4:7]},
            3,
            (0, 6),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[2:5],
                ("linear_history_root", "foo1"): RUN_BY_ID["linear_history_root"].metrics_values("foo1")[2:5],
                ("linear_history_fork1", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[2:5],
                ("linear_history_fork1", "foo1"): RUN_BY_ID["linear_history_root"].metrics_values("foo1")[2:5],
            },
            3,
            (0, 4),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[2:5],
                ("linear_history_root", "foo1"): RUN_BY_ID["linear_history_root"].metrics_values("foo1")[2:5],
                ("linear_history_fork1", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[2:5],
                ("linear_history_fork1", "foo1"): RUN_BY_ID["linear_history_root"].metrics_values("foo1")[2:5],
            },
            3,
            (None, 4),
            True,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"foo.*",
            {
                ("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[2:5],
                ("linear_history_root", "foo1"): RUN_BY_ID["linear_history_root"].metrics_values("foo1")[2:5],
            },
            3,
            (0, 4),
            False,
        ),
        (
            ["linear_history_root", "linear_history_fork1"],
            r"unique.*",
            {
                ("linear_history_root", "unique1/0"): RUN_BY_ID["linear_history_root"].metrics_values("unique1/0"),
                ("linear_history_fork1", "unique2/0"): RUN_BY_ID["linear_history_fork1"].metrics_values("unique2/0"),
            },
            None,
            (None, None),
            False,
        ),
        (
            r"^linear_history_(root|fork1)$",
            r"unique.*",
            {
                ("linear_history_root", "unique1/0"): RUN_BY_ID["linear_history_root"].metrics_values("unique1/0"),
                ("linear_history_fork1", "unique2/0"): RUN_BY_ID["linear_history_fork1"].metrics_values("unique2/0"),
            },
            None,
            (None, None),
            False,
        ),
        (
            r"^forked_history_fork1$",
            r"foo.*",
            {
                ("forked_history_fork1", "foo0"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo0")[1:4],
                ("forked_history_fork1", "foo1"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo1")[1:4],
            },
            3,
            (5, 10),
            False,
        ),
        (
            r"^forked_history_fork1$",
            r"foo.*",
            {
                ("forked_history_fork1", "foo0"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo0")[0:4],
                ("forked_history_fork1", "foo1"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo1")[0:4],
            },
            10,
            (None, None),
            False,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")[5:6]},
            None,
            (5, 5),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {},
            None,
            (100, 200),
            True,
        ),
        (
            r"^linear_history_root$",
            r"^foo0$",
            {("linear_history_root", "foo0"): RUN_BY_ID["linear_history_root"].metrics_values("foo0")},
            None,
            (0, 20),
            True,
        ),
        # Fetch last point of metrics for runs with forked history, including lineage to the root.
        (
            r"^forked_history_.*$",
            r"foo.*",
            {
                ("forked_history_root", "foo0"): RUN_BY_ID["forked_history_root"].metrics_values("foo0")[-1:],
                ("forked_history_root", "foo1"): RUN_BY_ID["forked_history_root"].metrics_values("foo1")[-1:],
                ("forked_history_fork1", "foo0"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo0")[-1:],
                ("forked_history_fork1", "foo1"): RUN_BY_ID["forked_history_fork1"].metrics_values("foo1")[-1:],
                ("forked_history_fork2", "foo0"): RUN_BY_ID["forked_history_fork2"].metrics_values("foo0")[-1:],
                ("forked_history_fork2", "foo1"): RUN_BY_ID["forked_history_fork2"].metrics_values("foo1")[-1:],
            },
            1,
            (None, None),
            True,
        ),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", ["absolute", None])
def test_fetch_run_metrics(
    new_project_id,
    runs_filter,
    attributes_filter,
    expected_metrics,
    type_suffix_in_column_names: bool,
    include_time: str,
    tail_limit: int,
    step_range: tuple[float, float],
    lineage_to_the_root: bool,
):
    df = runs.fetch_metrics(
        project=new_project_id,
        runs=runs_filter,
        attributes=attributes_filter,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_time=include_time,
        tail_limit=tail_limit,
        step_range=step_range,
        lineage_to_the_root=lineage_to_the_root,
    )

    expected_df = create_expected_data(
        new_project_id,
        expected_metrics,
        include_time=include_time,
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    pd.testing.assert_frame_equal(df, expected_df, check_dtype=False)


def create_expected_data(project, expected_metrics, include_time: str, type_suffix_in_column_names):
    data = {}
    for (run, metric_name), values in expected_metrics.items():
        attribute_run = identifiers.RunAttributeDefinition(
            run_identifier=identifiers.RunIdentifier(project_identifier=project, sys_id=identifiers.SysId(run)),
            attribute_definition=identifiers.AttributeDefinition(name=metric_name, type="float_series"),
        )
        rows = data.setdefault(attribute_run, [])

        for step, value in values:
            rows.append((int(timestamp_for_step(step).timestamp() * 1000), step, value, False, 1.0))

    sys_id_label_mapping = {identifiers.SysId(run): run for run, _ in expected_metrics.keys()}

    return create_metrics_dataframe(
        metrics_data=data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        index_column_name="run",
    )
