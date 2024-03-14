# flake8: noqa
from behave import (
    given,
    then,
    when,
)


@given("we have a read-only project")
@when("we initialize the read-only project")
def step_impl(context):
    # Imports
    from neptune_fetcher import ReadOnlyProject

    # Given
    context.project = ReadOnlyProject()


@then("no exception is thrown")
def step_impl(context):
    assert context.failed is False


@when("we list runs")
def step_impl(context):
    context.runs = list(context.project.list_runs())


@then("runs list is not empty")
def step_impl(context):
    assert context.runs != []


@then("runs list contains the run details we created")
def step_impl(context):
    assert sorted(context.runs, key=lambda kv: kv["sys/id"]) == [
        {"sys/id": "FETCH-1", "sys/name": None},
        {"sys/id": "FETCH-2", "sys/name": None},
    ]
