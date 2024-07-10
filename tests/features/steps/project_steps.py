# flake8: noqa
import pytest
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
    context.project_key = context.project._project_key  # noqa


@then("no exception is thrown")
def step_impl(context):
    assert context.failed is False


@when("we list runs")
def step_impl(context):
    context.runs = list(context.project.list_runs())


@when("we list experiments")
def step_impl(context):
    context.experiments = list(context.project.list_experiments())


@then("runs list is not empty")
def step_impl(context):
    assert context.runs != []


@then("experiments list is not empty")
def step_impl(context):
    assert context.experiments != []


@then("runs list contains the run details we created")
def step_impl(context):
    assert sorted(context.runs, key=lambda kv: kv["sys/id"]) == [
        {"sys/id": f"{context.project_key}-1", "sys/custom_run_id": "fetcher-aa-1"},
        {
            "sys/id": f"{context.project_key}-2",
            "sys/custom_run_id": "fetcher-bb-2",
        },
    ]


@then("experiments list contains the experiment details we created")
def step_impl(context):
    assert sorted(context.experiments, key=lambda kv: kv["sys/id"]) == [
        {
            "sys/id": f"{context.project_key}-1",
            "sys/custom_run_id": "fetcher-aa-1",
            "sys/name": "my-lovely-experiment",
        },
    ]
