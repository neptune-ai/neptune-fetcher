# Neptune Query Language (NQL)

When fetching runs from your project, use the Neptune Query Language (NQL) to filter the runs by an attribute and other criteria.

## NQL usage

Use the `query` argument of `fetch_runs_df()` and `fetch_experiments_df()` methods to pass a raw NQL string:

```py
from neptune_fetcher import ReadOnlyProject

project = ReadOnlyProject("workspace/project")

project.fetch_runs_df(
    query='(last(`accuracy`:floatSeries) > 0.88) AND (`learning_rate`:float < 0.01)'
)
```

## NQL syntax

An NQL query has the following parts:

```g4
`<attribute name>`:<attributeType> <OPERATOR> <zero or more values>
```

For example:

```g4
`scores/f1`:float >= 0.60
```

> [!IMPORTANT]
> Single quotes aren't supported inside the `query` string. If wrapping values with quote marks, use double quotes:
>
> ```g4
> query='`sys/description`:string = "test on new data"'
> ```

### Building a query

The following steps walk you through constructing each part of a valid query:

#### 1. Attribute name

```g4 title='Your query'
`scores/f1`
```

Use the attribute name that you specified when assigning the metadata to the run. For the above example, it's `run.log_configs({"scores/f1": f1_score})`.

While usually not necessary, it's safest to enclose the attribute name in single backquotes (`).

#### 2. Attribute type

```g4 title='Your query'
`scores/f1`:float
```

For Neptune to correctly parse the specified attribute name, provide the Neptune [attribute type][attribute-types] immediately after the attribute name and separate them by a colon (`:`). The attribute type must be in camel case.

Available attribute types: `float`, `string`, `bool`, `datetime`, `floatSeries`, `stringSet`, `int`

> You must specify the attribute type to distinguish runs that may have the same attribute name but different data types.

#### 3. Operator'

```g4 title='Your query'
`scores/f1`:float >
```

The operator depends on the attribute type:

| Operators                | Supported attribute types |
| ------------------------ | --------------------- |
| `=` ,`!=`                | `bool`, `experimentState`, `string`, `int`, `float`, `floatSeries` [aggregates](#aggregate-functions-of-numerical-series) |
| `>`, `>=`, `<`, `<=`     | `int`, `float`, `floatSeries` aggregates |
| `CONTAINS`               | `string`, `stringSet` |
| `MATCHES`, `NOT MATCHES` | `string`              |
| `EXISTS`                 | Any                   |
| `NOT`                    | Negates other operators or clauses. See [Negation](#negation). |

#### 4. Value

```g4 title='Your query'
`scores/f1`:float > 0.8
```

It's usually possible to enter the plain value without quotes, but in some cases double quotes `""` are necessary. For example, if the value contains a space.

```g4 title='Enclosing a special value in quotes'
query='`sys/tags`:stringSet CONTAINS "my tag"'
```

#### (Optional) 5. Statistical functions

If your attribute is a float series, wrap the first part of the expression in a supported aggregate function: `average()`, `last()`, `max()`, or `min()`.

```g4 title='Example query'
average(`accuracy`:floatSeries) > 0.8
```

### Multi-clause queries

You can build a complex query, in which multiple conditions are joined by logical operators.

Surround the clauses with `()` and use `AND` or `OR` to join them:

```g4
(`attribute1`:attributeType = value1) AND (`attribute2`:attributeType = value2)
```

```g4 title="Example: Particular learning rate and high enough final accuracy"
query='(last(`metrics/acc`:floatSeries) >= 0.85) AND (`learning_rate`:float = 0.002)'
```

Note that each run is matched against the full query individually.

### Negation

You can use `NOT` in front of operators or clauses.

The following are equivalent and both exclude runs that have "blobfish" in their name:

```g4
`sys/name`:string NOT CONTAINS "blobfish"
```

```g4
NOT `sys/name`:string CONTAINS "blobfish"
```

You can also negate joined clauses by enclosing them with parentheses:

```g4 title='Example: Exclude failed runs whose names contain "blobfish"'
NOT (`sys/name`:string CONTAINS blobfish AND `sys/failed`:bool = True)
```

### Aggregate functions of numerical series

You can use the following statistical (aggregate) functions on <FloatSeries /> attributes:

- `average()`
- `last()`
- `max()`
- `min()`
- `variance()`

For example, to filter by the last logged score of a float series attribute with the path `metrics/accuracy`, use:

```g4
last(`metrics/accuracy`:floatSeries) >= 0.80
```

### Examples

**Models small enough to be used on mobile that have decent test accuracy**

```g4 title="NQL query"
(`model_info/size_MB`:float <= 50MB) AND (last(`test/acc`:floatSeries) > 0.90)
```

```py title="What was logged"
run = Run(...)
run.log_configs({"model_info/size_MB": 45})
for epoch in epochs:
    # training loop
    acc = ...
    run.log_metrics({"test/acc": acc})
```

**All of Jackie's runs from the current exploration task**

```g4 title="NQL query"
(`sys/owner`:string = "jackie") AND (`sys/tags`:stringSet CONTAINS "exploration")
```

```py title="What was logged"
run.add_tags=(tags=["exploration", "pretrained"])
```

**All failed runs from the start of the year**

```g4 title="NQL query"
(sys/creation_time:datetime > "2024-01-01T00:00:00Z") AND (sys/failed:bool = True)
```

```py title="What was logged"
# Date is in 2024
run = Run(...)
# Exception was raised during execution
```

## Supported data types

See example queries for the supported data types.

### Float

To query float values, use:

```py title="Retrieve runs with F1 score lower than 0.5"
project.fetch_runs_df(
    query="`f1_score`:float < 0.50"
)
```

In this case, the logging code could be something like `run.log_configs({"f1_score": 0.48})` for a run matching the expression.

#### Float series

To obtain a value that characterizes a series of values, use an aggregate function:

```g4 title="Filter by last appended accuracy score"
last(`metrics/accuracy`:floatSeries) >= 0.80
```

The following statistical functions are supported:

- `average()`
- `last()`
- `max()`
- `min()`
- `variance()`

### String

You can filter either by the full string, or use the `CONTAINS` operator to access substrings.

```py title="Exact match"
project.fetch_runs_df(
    query='`sys/name`:string = "cunning-blobfish"'
)
```

```py title="Partial match (contains substring)"
project.fetch_runs_df(
    query='`sys/name`:string CONTAINS "blobfish"'
)
```

> See also [Name](#name).

To match against a regular expression, use the operators `MATCHES` and `NOT MATCHES`:

```py title="Matches regex"
project.fetch_runs_df(
    query=r'`parameters/optimizer`:string MATCHES "Ada\\w+"'
)
```

```py title="Doesn't match regex"
project.fetch_runs_df(
    query=r'`parameters/optimizer`:string NOT MATCHES "Ada\\w+"'
)
```

> [!NOTE]
>
> When using regex with the `query` argument, you must escape backslashes and quotes in the pattern. In this case, using a raw Python string is less cluttered than passing a regular string:
>
> ```py title="Escaping characters in regular Python string"
> project.fetch_runs_df(
>     query=r'`parameters/optimizer`:string MATCHES "Ada\\\\w+"'
> )
> ```

### Tags

Tags are stored as a <StringSet /> in the auto-created `sys/tags` attribute. To filter by one or more tags, this is the attribute you need to access.

```py title="Query by single tag"
project.fetch_runs_df(
    query='`sys/tags`:stringSet CONTAINS "tag-name"'
)
```

```g4 title="Query by multiple tags: Matches at least one tag (OR)"
(`sys/tags`:stringSet CONTAINS "tag1") OR (`sys/tags`:stringSet CONTAINS "tag2")
```

```g4 title="Query by multiple tags: Matches all tags (AND)"
(`sys/tags`:stringSet CONTAINS "tag1") AND (`sys/tags`:stringSet CONTAINS "tag2")
```

### System metadata

The system namespace (`sys`) automatically stores basic metadata about the environment and run. Most of the values are simple string, float, or Boolean values.

#### Date and time

Neptune automatically creates three timestamp attributes:

- `sys/creation_time`: When the run object was first created.
- `sys/modification_time`: When the object was last modified. For example, a tag was removed or some metadata was logged.
- `sys/ping_time`: When the object last interacted with the Python client library. That is, something was logged or modified through the code.

For the value, enter a combined date and time representation with a time-zone specification, in ISO 8601 format:

```
YYYY-MM-DDThh:mm:ssZ
```

Where `Z` is the time-zone offset for UTC. You can use a different offset.

```g4 title="Pinged by the Python client after 5 AM UTC on a specific date"
`sys/ping_time`:datetime > "2024-02-06T05:00:00Z"
```

```g4 title="Pinged by the Python client after 5 AM Japanese time on a specific date"
`sys/ping_time`:datetime > "2024-02-06T05:00:00+09"
```

You can also enter relative time values:

- `-2h` (last 2 hours)
- `-5d` (last 5 days)
- `-1M` (last month)

```g4 title="Created more than 3 months ago"
`sys/creation_time`:datetime < "-3M"
```

#### Description

To filter runs by the description, use:

```py title="Exact match"
project.fetch_runs_df(
    query='`sys/description`:string = "test run on new data"'
)
```

```py title="Partial match (contains substring)"
project.fetch_runs_df(
    query='`sys/description`:string CONTAINS "new data"'
)
```

#### ID

To filter runs by their Neptune ID, use:

```py title="Single run"
project.fetch_runs_df(
    query='`sys/id`:string = "NLI-345"'
)
```

To fetch multiple specific runs at once, use the `OR` operator:

```py title="Multiple runs"
project.fetch_runs_df(
    query='(`sys/id`:string = "NLI-35") OR (`sys/id`:string = "NLI-36")'
)
```

#### Name

To filter experiments by their name, use:

```py title="Exact match"
project.fetch_runs_df(
    query='`sys/name`:string = "cunning-blobfish"'
)
```

```py title="Partial match (contains substring)"
project.fetch_runs_df(
    query='`sys/name`:string CONTAINS "blobfish"'
)
```

You can also use a regular expression to match experiment names. In this case, instead of `query`, use the `names_regex` parameters:

```py title="Include experiments that match regex"
specific_experiments_df = project.fetch_experiments_df(
    names_regex=r"astute-.+-135"
)
```

```py title="Exclude experiments that match regex"
specific_experiments_df = project.fetch_experiments_df(
    names_exclude_regex=r"experiment-\d{2,4}"
)
```

> [!NOTE]
> Neptune uses the RE2 regular expression library. For supported regex features and limitations, see the [RE2 syntax guide][re2-syntax].

#### Owner

To filter by the user or service account that created the run, use:

```py title="By owner: Regular username"
project.fetch_runs_df(
    query='`sys/owner`:string = "jackie"'
)
```

```py title="By one of the workspace service accounts"
project.fetch_runs_df(
    query='`sys/owner`:string CONTAINS "@ml-team"'
)
```

In this case, the expression matches all service account names that belong to the workspace **ml-team**.

#### State

To fetch only closed runs, use:

```py title="Fetch inactive runs"
project.fetch_runs_df(
    query='`sys/state`:experimentState = "inactive"'
)
```

#### Failed status

If an exception occurred during the run, it's set as "Failed". In practice, it means the `sys/failed` attribute is set to `True`.

```py title="Fetch failed runs"
project.fetch_runs_df(
    query='`sys/failed`:bool = True'
)
```

---

> **Related documentation:**
>
> - [Runs table][runs-table]
> - [Attribute types reference][attribute-types]


[attribute-types]: https://docs.neptune.ai/attribute_types
[re2-syntax]: https://github.com/google/re2/wiki/syntax
[runs-table]: https://docs.neptune.ai/runs_table
