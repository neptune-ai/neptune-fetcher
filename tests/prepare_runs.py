from ingestion import NeptuneIngestion

client = NeptuneIngestion(
    project="...",
    api_token="...",
    kafka_config={"bootstrap_servers": ["neptune-kafka:9092"]},
    kafka_topic="ingest.feed",
    number_of_partitions=2,
)

run1_id = client.create_run()
for i in range(3):
    client.log_metrics(
        run1_id,
        i,
        {"series/float": 2.0**i},
        run1_id,
    )

client.log_fields(run1_id, {"fields/int": 5, "fields/float": 3.14, "fields/string": "Neptune Rulez!"}, run1_id)


run2_id = client.create_run()
client.log_fields(run2_id, {"fields/int": -4, "fields/float": 0.2, "fields/string": "No, you rulez!"}, run2_id)
