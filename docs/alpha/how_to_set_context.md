# Set context for Fetcher Alpha API

To work with multiple projects simultaneously, use contexts. This way, you can set the scope for individual fetching calls or globally for your session.

- To set a new project or API token globally, use `set_project()` or `set_api_token()`:

    ```python
    import neptune_fetcher.alpha as npt


    npt.set_project("some-workspace/another-project")
    ```

- To create a context object that you can pass to the `context` argument of a fetching method, use `get_context()` to copy the global context and set a different project or API token:

    ```python
    another_project_context = npt.get_context().with_project("some-workspace/another-project")
    npt.list_experiments(r"exp_.*", context=another_project_context)
    ```

## Example flow

1. `NEPTUNE_PROJECT` environment variable is read on module initialization:

    ```python
    import neptune_fetcher.alpha as npt
    ```

1. Work on the default project inferred from the environment variables:

    ```python
    npt.list_experiments(r"exp_.*")
    ```

    ```pycon title="Output"
    ['exp_dhxxz', 'exp_saazg', 'exp_sxlrq', 'exp_vlqfe', 'exp_fpogj']
    ```

1. Work on another project without changing the global context:

    ```python
    another_project_ctx = npt.get_context().with_project("some-workspace/another-project")
    npt.list_experiments(r"exp_.*", context=another_project_ctx)
    ```

    ```pycon title="Output"
    ['exp_oecez', 'exp_ytbda', 'exp_khfym']
    ```

1. Change the project globally:

    ```python
    npt.set_project("some-workspace/another-project")
    ```

1. Do some more work on another project:

    ```python
    npt.list_experiments(r"exp_.*")
    ```

    ```pycon title="Output"
    ['exp_oecez', 'exp_ytbda', 'exp_khfym']
    ```
