
import json
import os
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from shared.storage import get_s3_client

from shared.parquet_upload import upload_parquet_batch

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

TOPIC = "ami-readings-stream"
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = "bronze/streaming"
BATCH_SIZE = 5000


def main():
    s3 = get_s3_client()
    s3.head_bucket(Bucket=BUCKET)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "_" + uuid.uuid4().hex[:8]
    batch = []
    total = 0
    uploaded = 0
    batch_number = 0

    try:
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            raise RuntimeError(f"No partitions found for topic {TOPIC}")

        assigned = [TopicPartition(TOPIC, p) for p in sorted(partitions)]
        consumer.assign(assigned)
        consumer.seek_to_beginning(*assigned)

        # Capture the end offsets for each partition.
        end_offsets = consumer.end_offsets(assigned)
        start_offsets = {tp: 0 for tp in assigned}
        print("Partition targets:")
        for tp in assigned:
            print(f"  Partition {tp.partition}: {end_offsets[tp]:,} messages")

        print(f"\nWriting to run_id={run_id}")

        while True:
            unfinished = [
                tp for tp in assigned
                if consumer.position(tp) < end_offsets[tp]
            ]
            if not unfinished:
                break

            records = consumer.poll(timeout_ms=1000, max_records=2000)
            if not records:
                continue

            for tp, messages in records.items():
                for message in messages:
                    if message.offset >= end_offsets[tp]:
                        continue

                    record = message.value
                    if not isinstance(record, dict):
                        raise ValueError("Kafka message is not a JSON object.")
                    if not record.get("reading_id") or not record.get("meter_id"):
                        raise ValueError("Missing reading_id or meter_id.")

                    batch.append(record)
                    total += 1

                    if len(batch) >= BATCH_SIZE:
                        uploaded += upload(s3, batch, run_id, batch_number)
                        batch_number += 1
                        batch.clear()
                        print(f"Read {total:,}; uploaded {uploaded:,}")

        if batch:
            uploaded += upload(s3, batch, run_id, batch_number)
            batch.clear()

        expected = sum(end_offsets[tp] - start_offsets[tp] for tp in assigned)
        print("\nFinished reading partition ranges.")
        print(f"Expected messages: {expected:,}")
        print(f"Messages read:     {total:,}")
        print(f"Rows uploaded:     {uploaded:,}")
        print(f"Destination: s3://{BUCKET}/{PREFIX}/run_id={run_id}/")

        if total != expected or uploaded != expected:
            raise RuntimeError(
                f"Count mismatch: expected {expected}, read {total}, uploaded {uploaded}"
            )

        print("VERIFICATION PASSED")

    finally:
        consumer.close()


def upload(s3, records, run_id, batch_number):
    count, _key = upload_parquet_batch(
        s3, records, bucket=BUCKET,
        key_prefix=f"{PREFIX}/run_id={run_id}/",
        local_dir=ROOT / "data",
        filename_prefix="recovery_",
        batch_number=batch_number, index_width=5, random_key_suffix=True,
    )
    return count


if __name__ == "__main__":
    main()
