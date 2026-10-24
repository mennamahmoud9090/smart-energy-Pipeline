# Smart Energy Intelligence Platform

SIC 8 Big Data graduation project for synthetic Advanced Metering Infrastructure (AMI) readings. The implemented repository currently provides a data generator, Kafka publishers/consumers, MinIO/S3 upload and validation scripts, Docker Compose infrastructure, and a draft data contract.

> All data is **synthetic**. Nothing here is real EEHC telemetry. Spark schemas are defined in `shared/schemas.py`, but a complete Spark/Airflow/Snowflake processing pipeline is not implemented in this repository.

## Prerequisites

- Python 3.10+ (use the version supported by your installed dependency wheels)
- Docker Engine/Desktop with Docker Compose v2
- Kafka and MinIO are required for publisher, consumer, upload, and smoke scripts.

## Environment setup

```bash
cp .env.example .env
# Edit .env and set a non-default local S3_ROOT_PASSWORD.
python -m venv .venv
# Activate the virtual environment, then:
python -m pip install -r requirements-dev.txt
```

`.env` is local-only and must not be committed. `.env.example` contains safe example values; replace the placeholder password before starting services.

## Start infrastructure

```bash
docker compose config
docker compose up -d
docker compose ps
docker compose logs kafka-init minio-init
```

Services and local endpoints:

| Service | URL / address |
|---|---|
| MinIO web console | http://localhost:9001 |
| MinIO S3 API | http://localhost:9000 |
| Kafka UI | http://localhost:8081 |
| Kafka from the host | `localhost:9092` |

Stop services without deleting persistent data with `docker compose down`. Do not add `-v` unless you intentionally want to delete Docker volumes and stored data.

## Run the generator

Run modules from the repository root:

```bash
python -m generator.historical_generator --help
python -m generator.publish_to_kafka --help
python -m generator.publish_continuous_to_kafka --help
python -m generator.upload_to_minio --help
```

The generator reads scale settings such as `GEN_SEED`, `GEN_NUM_METERS`, `GEN_NUM_DAYS`, `GEN_START_DATE`, and `GEN_METERS_PER_TRANSFORMER` from `.env`. Some publisher modes have fixed large target expectations; read the script help and the known limitations below before running them.

## Consumers and operational scripts

Run these commands from the repository root, with Kafka and MinIO configured and running:

```bash
python -m consumer.stream_to_minio
python -m consumer.stream_to_minio_continuous
python -m consumer.recover_stream_to_minio
python -m scripts.validate_minio_bronze --run-id 20261023T124246Z
python -m scripts.validate_dataset --help
```

The smoke scripts contact real local services and may publish messages or write objects. They are intentionally outside pytest discovery; run them only when you intend to interact with your local Kafka/MinIO instance.

```bash
python -m scripts.smoke_seed_dev_data
python -m scripts.smoke_kafka_sample
```

## Tests and checks

```bash
python -m pytest
python -m compileall -q generator consumer shared scripts tests
```

Unit tests exercise the shared contracts and JSON serialization. Kafka/MinIO integration checks require the services to be running and valid local `.env` settings.

## Repository layout

```text
.
├── consumer/                 # Kafka-to-MinIO ingestion and recovery
├── docker/
│   └── init/                 # MinIO bucket and Kafka topic initialization
├── docs/
│   └── schema.md             # Draft data contract; team decisions are listed there
├── generator/                # Python modules runnable with python -m generator.<module>
├── scripts/                  # Smoke and dataset/MinIO validation commands
├── shared/                   # Shared schemas, contracts, serialization, S3 client
├── tests/
│   └── unit/                 # Unit tests; no live-service scripts
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── pyproject.toml
```

## Known limitations and team decisions

- Kafka topic names are inconsistent across scripts (`ami-readings`, `ami-readings-dev`, and `ami-readings-stream`); `docker/init/create_topics.sh` creates only the configured `TOPIC_READINGS` (default `ami-readings`). Confirm intended topic routing before running a publisher/consumer combination.
- Some publishers require exactly 100,000 meters, while `.env.example` defaults `GEN_NUM_METERS=1000`. Confirm whether large-scale scripts should support prototype-scale input.
- Streaming `reading_id` values use `sim-<n>-<meter>`, while `docs/schema.md` specifies `READ_<meter_id>_<YYYYMMDDHHMM>`. This is a data-contract decision; no behavior was changed.
- Consumer scripts use random/default consumer group behavior and do not consistently commit offsets. This affects replay and recovery semantics; review before using beyond local demos.
- Streaming Bronze output paths differ from the documented shared date-partitioned Bronze path. Confirm the desired layout with the team.
- `generator/validate.py` appears to validate a previous generated-dataset layout and is not wired into the current generation flow. It is retained pending team approval.
- This repository does not currently implement the end-to-end Spark/Airflow/Snowflake/BI pipeline.

## Docker networking

Services use the `smart-energy-net` Docker network. Containers attached to this network should use `kafka:29092` and `http://minio:9000`; host-run Python scripts use `localhost:9092` and `http://localhost:9000`.
