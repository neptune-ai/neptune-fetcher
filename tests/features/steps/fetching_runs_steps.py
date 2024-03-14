# flake8: noqa
from behave import (
    given,
    then,
    when,
)


@given("we filter by `with_ids`")
def step_impl(context):
    context.kwargs = {"with_ids": ["FETCH-1"]}


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


@when("we fetch runs dataframe")
def step_impl(context):
    if hasattr(context, "kwargs"):
        context.dataframe = context.project.fetch_runs_df(**context.kwargs)
    else:
        context.dataframe = context.project.fetch_runs_df()


@then("we should get 1 run")
def step_impl(context):
    assert len(context.dataframe) == 1


@then("we should get 2 runs")
def step_impl(context):
    assert len(context.dataframe) == 2


@then("we should get 2 runs with 1 column")
def step_impl(context):
    assert context.column in context.dataframe.columns
    assert len(context.dataframe.columns) == 1 + 1  # +1 for the run id column


@then("we should get 2 runs sorted by `fields/float` in descending order")
def step_impl(context):
    assert context.dataframe[context.column].is_monotonic_decreasing
