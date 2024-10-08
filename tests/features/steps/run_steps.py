# flake8: noqa
from behave import (
    given,
    then,
    when,
)


@when("we initialize the read-only run")
def step_impl(context):
    from neptune_fetcher.read_only_run import ReadOnlyRun

    context.run = ReadOnlyRun(with_id=f"{context.project_key}-1", read_only_project=context.project)


@when("we initialize the read-only run with custom id")
def step_impl(context):
    from neptune_fetcher.read_only_run import ReadOnlyRun

    context.run = ReadOnlyRun(custom_id="fetcher-aa-1", read_only_project=context.project)


@when("we initialize the read-only run with experiment name")
def step_impl(context):
    from neptune_fetcher.read_only_run import ReadOnlyRun

    context.run = ReadOnlyRun(experiment_name="my-lovely-experiment", read_only_project=context.project)


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


@given("we have a bool field")
def step_impl(context):
    context.field = "sys/failed"


@given("we have a datetime field")
def step_impl(context):
    context.field = "sys/creation_time"


@given("we have a string set field")
def step_impl(context):
    context.field = "sys/tags"


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
    assert sorted(context.field_names) == sorted(
        [
            "fields/float",
            "fields/int",
            "fields/string",
            "sys/custom_run_id",
            "sys/description",
            "sys/family",
            "sys/id",
            "sys/name",
            "sys/monitoring_time",
            "sys/owner",
            "sys/running_time",
            "sys/size",
            "sys/creation_time",
            "sys/ping_time",
            "sys/group_tags",
            "sys/failed",
            "sys/tags",
            "series/float",
            "sys/modification_time",
            "sys/relative_creation_time_ms",
            "sys/trashed",
            "sys/state",
        ]
    )


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


@then("the value is False")
def step_impl(context):
    assert context.value is False


@then("the value is `Neptune Rulez!`")
def step_impl(context):
    assert context.value == "Neptune Rulez!"


@then("the values are [1, 2, 4]")
def step_impl(context):
    assert context.values == [1, 2, 4]


@then("we have a value")
def step_impl(context):
    assert context.value is not None
