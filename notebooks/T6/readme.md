### Files in this folder

- **docker-compose.yaml**: Configuration for a local two-broker Kafka (KRaft) cluster and ksqlDB server used in Task 6.
- **producer.py**: Kafka producer that streams one calendar year of NYC TLC trips (Yellow + FHVHV) from Parquet in pickup-time order, emitting a unified JSON schema.
- **consumer.py**: Baseline Kafka consumer that computes rolling descriptive statistics by borough and for the top-N pickup zones.
- **clustream_consumer.py**: Extended consumer that performs online stream clustering (CluStream‑style). It maintains micro‑clusters with exponential decay and periodically summarizes them into macro‑clusters for reporting (optionally publishing summaries to a Kafka topic).
