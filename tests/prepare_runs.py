import time

from ingestion import NeptuneIngestion

client = NeptuneIngestion(
    project="...",
    api_token="...",
    kafka_config={"bootstrap_servers": ["localhost:9092"]},
    kafka_topic="ingest.feed",
)

run1_id = client.create_run(run_id="fetcher-aa-1", experiment_id="my-lovely-experiment")
for i in range(3):
    client.log(
        run_id=run1_id,
        step=i,
        metrics={"series/float": 2.0**i},
        run_family=run1_id,
    )

client.log(
    run_id=run1_id,
    fields={"fields/int": 5, "fields/float": 3.14, "fields/string": "Neptune Rulez!"},
    run_family=run1_id,
)

time.sleep(5)

run2_id = client.create_run(run_id="fetcher-bb-2")
client.log(
    run_id=run2_id,
    fields={"fields/int": -4, "fields/float": 0.2, "fields/string": "No, you rulez!"},
    run_family=run2_id,
)

client.close()
