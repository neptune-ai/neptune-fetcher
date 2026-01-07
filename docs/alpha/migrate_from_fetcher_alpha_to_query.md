# Migrate from Fetcher Alpha to Query API

To update your code from `neptune_fetcher.alpha` to `neptune_query`:

1. Instead of `neptune-fetcher`, install `neptune-query`:

    ```sh
    pip install "neptune-query<2.0.0"
    ```

1. Change the import statements to the following:

    ```py
    import neptune_query as nq
    import neptune_query.runs as nq_runs
    from neptune_query.filters import Filter, Attribute, AttributeFilter
    ```

1. To override the Neptune project or API token set by environment variables, use the following instead of the `Context` class:

    - To set the API token for the session, call `nq.set_api_token("SomeNeptuneApiToken")`.
    - To set the project, pass the project path to the `project` argument of any fetching method.

1. The `aggregation` argument of `Attribute` and `AttributeFilter` is removed. Change your script to no longer use this parameter.

1. Because the extended syntax is supported for regular expressions, some filter options have been streamlined.

    - The following `Filter` methods have been replaced:

        - `matches_all()`, `matches_none()` &rarr; `matches()`
        - `name_in()`, `name_eq` &rarr; `name()`

    - The following `AttributeFilter` parameters have been replaced:

        - `name_eq`, `name_in`, `name_matches_all`, `name_matches_none` &rarr; `name`
        - `type_in` &rarr; `type`

1. The `Filter.any()` and `Filter.all()` methods are removed. To join filters, use the operators `|` and `&` instead.
