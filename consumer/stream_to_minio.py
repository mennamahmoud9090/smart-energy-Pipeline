
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

from shared.storage import get_s3_client
from shared.parquet_upload import upload_parquet_batch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = "ami-readings"
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = "bronze/streaming"
BATCH_SIZE = 5_000
TARGET_MESSAGES = 1_066


def main():
    s3 = get_s3_client()

    # Verify that the target bucket is accessible.
    s3.head_bucket(Bucket=BUCKET)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    consumer.subscribe([TOPIC])

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    batch = []
    received = 0
    uploaded = 0

    print(f"Topic: {TOPIC}")
    print(f"Target: {TARGET_MESSAGES:,} messages")
    print(f"Destination: s3://{BUCKET}/{PREFIX}/")
    print("Waiting for Kafka assignment...")

    try:
        # Wait for Kafka to assign partitions.
        deadline = time.monotonic() + 30

        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)

            if time.monotonic() > deadline:
                raise TimeoutError(
                    "Kafka partitions were not assigned."
                )
        # Start reading from the beginning of every assigned partition.
        partitions = list(consumer.assignment())
        consumer.seek_to_beginning(*partitions)

        print(f"Assigned partitions: {len(partitions)}")
        print("Reading from the beginning of the topic.")
        last_progress = time.monotonic()

        while received < TARGET_MESSAGES:
            records = consumer.poll(
                timeout_ms=1000,
                max_records=1000,
            )

            if records:
                last_progress = time.monotonic()

            elif time.monotonic() - last_progress > 15:
                print(
                    f"No new messages for 15 seconds. "
                    f"Received {received:,}/{TARGET_MESSAGES:,}. "
                    "Flushing remaining batch."
                )
                break

            for messages in records.values():
                for message in messages:
                    record = message.value

                    if not isinstance(record, dict):
                        raise ValueError(
                            "Kafka message is not a JSON object."
                        )

                    if (
                        not record.get("reading_id")
                        or not record.get("meter_id")
                    ):
                        raise ValueError(
                            "Message is missing reading_id or meter_id."
                        )

                    batch.append(record)
                    received += 1

                    if len(batch) >= BATCH_SIZE:
                        uploaded += upload_batch(
                            s3,
                            batch,
                            run_id,
                            uploaded // BATCH_SIZE,
                        )
                        batch.clear()

                        print(
                            f"Received {received:,}/"
                            f"{TARGET_MESSAGES:,}; "
                            f"uploaded {uploaded:,}"
                        )

                    if received >= TARGET_MESSAGES:
                        break

                if received >= TARGET_MESSAGES:
                    break

        # Upload any remaining records, even if fewer than BATCH_SIZE.
        if batch:
            uploaded += upload_batch(
                s3,
                batch,
                run_id,
                uploaded // BATCH_SIZE,
            )
            batch.clear()

        print("\nIngestion completed.")
        print(f"Messages received: {received:,}")
        print(f"Messages uploaded: {uploaded:,}")
        print(
            f"Destination: "
            f"s3://{BUCKET}/{PREFIX}/run_id={run_id}/"
        )

    finally:
        consumer.close()


def upload_batch(s3, records, run_id, batch_number):
    count, key = upload_parquet_batch(
        s3,
        records,
        bucket=BUCKET,
        key_prefix=f"{PREFIX}/run_id={run_id}/",
        local_dir=PROJECT_ROOT / "data",
        filename_prefix="stream_batch_",
        batch_number=batch_number,
        index_width=5,
        random_key_suffix=True,
    )

    print(f"Uploaded {count:,} rows -> s3://{BUCKET}/{key}")
    return count


if __name__ == "__main__":
    main()
