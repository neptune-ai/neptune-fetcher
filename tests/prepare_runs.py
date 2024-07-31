from neptune_scale import Run

with Run(
    api_token="...", project="...", run_id="fetcher-aa-1", family="fetcher-aa-1", as_experiment="my-lovely-experiment"
) as run:
    for i in range(3):
        run.log(
            step=i,
            metrics={"series/float": 2.0**i},
        )

    run.log(
        fields={"fields/int": 5, "fields/float": 3.14, "fields/string": "Neptune Rulez!"},
    )

with Run(api_token="...", project="...", run_id="fetcher-bb-2", family="fetcher-bb-2") as run:
    run.log(
        fields={"fields/int": -4, "fields/float": 0.2, "fields/string": "No, you rulez!"},
    )
