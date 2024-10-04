# Building docker image

Note that the image does not need to be built from scratch before each run,
because the testing script updates all the necessary dependencies
(git repositories & pip packages) that were pulled in during the build phase.

```shell
docker build -t . neptune-pye2e
```

# Running docker image

You need to provide:

* `NEPTUNE_WORKSPACE` - name of the workspace where temporary projects are created.
  The projects is deleted upon test completion.
* `NEPTUNE_API_TOKEN` - API token for the workspace

Run all tests:

```shell
docker run -e NEPTUNE_WORKSPACE=test-workspace -e NEPTUNE_API_TOKEN=... neptune-pye2e
```

JUnit test reports are written to `/reports/**/test-results.xml`.
