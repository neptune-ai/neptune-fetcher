# flake8: noqa
from behave import (
    given,
    then,
    when,
)


@when("we initialize the read-only run")
def step_impl(context):
    from neptune_fetcher.read_only_run import ReadOnlyRun

    context.run = ReadOnlyRun(with_id="FETCH-1", read_only_project=context.project)


@given("we have a read-only run")
def step_impl(context):
    context.execute_steps("given we have a read-only project")
    context.execute_steps("when we initialize the read-only run")


@given("we have an integer field")
def step_impl(context):
    context.field = "fields/int"


@given("we have a float field")
def step_impl(context):
    context.field = "fields/float"


@given("we have a string field")
def step_impl(context):
    context.field = "fields/string"


@given("we have a float series")
def step_impl(context):
    context.series = "series/float"


@when("we fetch the field value")
def step_impl(context):
    context.value = context.run[context.field].fetch()


@when("we fetch the field names")
def step_impl(context):
    context.field_names = list(context.run.field_names)


@then("all field names are present")
def step_impl(context):
    assert sorted(context.field_names) == [
        "fields/float",
        "fields/int",
        "fields/string",
        "series/float",
        "sys/creation_time",
        "sys/id",
        "sys/modification_time",
        "sys/monitoring_time",
        "sys/ping_time",
        "sys/running_time",
        "sys/size",
        "sys/state",
    ]


@when("we fetch the series values")
def step_impl(context):
    context.values = list(context.run[context.series].fetch_values().value)


@when("we fetch the series last value")
def step_impl(context):
    context.value = context.run[context.series].fetch_last()


@then("the value is 3.14")
def step_impl(context):
    assert context.value == 3.14


@then("the value is 4")
def step_impl(context):
    assert context.value == 4


@then("the value is 5")
def step_impl(context):
    assert context.value == 5


@then("the value is `Neptune Rulez!`")
def step_impl(context):
    assert context.value == "Neptune Rulez!"


@then("the values are [1, 2, 4]")
def step_impl(context):
    assert context.values == [1, 2, 4]
