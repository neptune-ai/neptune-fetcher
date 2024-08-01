# flake8: noqa
from behave import (
    given,
    then,
    when,
)


@given("we filter by `with_ids`")
def step_impl(context):
    context.kwargs = {"with_ids": [f"{context.project_key}-2"]}


@given("we limit the number of runs to 1")
def step_impl(context):
    context.kwargs = {"limit": 1}


@given("we select only 1 column")
def step_impl(context):
    context.column = "fields/float"
    context.kwargs = {"columns": [context.column]}


@given("we sort by `fields/float` by descending order")
def step_impl(context):
    context.column = "fields/float"
    context.kwargs = {"sort_by": context.column, "ascending": False}


@given("we select columns by regex")
def step_impl(context):
    context.expected_columns = ["fields/int", "fields/float", "fields/string"]
    context.kwargs = {"columns_regex": "fields/.*", "columns": []}


@given("we filter by run names regex")
def step_impl(context):
    context.kwargs = {"names_regex": "my-.*-experiment"}


@given("we filter by custom id regex")
def step_impl(context):
    context.kwargs = {"custom_id_regex": "fetcher-bb-.*"}


@when("we fetch runs dataframe")
def step_impl(context):
    if hasattr(context, "kwargs"):
        context.dataframe = context.project.fetch_runs_df(**context.kwargs)
    else:
        context.dataframe = context.project.fetch_runs_df()


@when("we fetch experiment dataframe")
def step_impl(context):
    if hasattr(context, "kwargs"):
        context.dataframe = context.project.fetch_experiments_df(**context.kwargs)
    else:
        context.dataframe = context.project.fetch_experiments_df()


@then("we should get 1 run and 1 experiment")
def step_impl(context):
    assert len(context.dataframe) == 2


@then("we should get 1 run")
def step_impl(context):
    assert len(context.dataframe) == 1


@then("we should get first run")
def step_impl(context):
    assert len(context.dataframe) == 1
    assert context.dataframe["sys/id"].values[0] == f"{context.project_key}-1"


@then("we should get second run")
def step_impl(context):
    assert len(context.dataframe) == 1
    assert context.dataframe["sys/id"].values[0] == f"{context.project_key}-2"


@then("we should have selected columns included")
def step_impl(context):
    # run id column is always included
    assert sorted(
        context.expected_columns
        + [
            "sys/id",
            "sys/custom_run_id",
        ]
    ) == sorted(context.dataframe.columns.tolist())


@then("we should get 2 runs with 1 column")
def step_impl(context):
    assert context.column in context.dataframe.columns
    assert len(context.dataframe.columns) == 1 + 2  # +2 for the run id column and custom id column


@then("we should get 2 runs sorted by `fields/float` in descending order")
def step_impl(context):
    assert context.dataframe[context.column].is_monotonic_decreasing
