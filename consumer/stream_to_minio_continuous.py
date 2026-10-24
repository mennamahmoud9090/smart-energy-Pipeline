
import json
import os
import signal
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

from shared.storage import get_s3_client

from shared.parquet_upload import upload_parquet_batch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.getenv("KAFKA_INGEST_TOPIC", "ami-readings-dev")
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = "bronze/streaming"
BATCH_SIZE = 5_000

STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def upload_batch(s3, records, run_id, batch_number):
    """Write one batch to Parquet and upload it to MinIO."""
    count, key = upload_parquet_batch(
        s3, records, bucket=BUCKET,
        key_prefix=f"{PREFIX}/run_id={run_id}/",
        local_dir=PROJECT_ROOT / "data" / "stream_batches",
        filename_prefix="batch_",
        batch_number=batch_number, index_width=6, random_key_suffix=False,
    )
    print(
        f"Uploaded batch {batch_number}: {count:,} rows "
        f"-> s3://{BUCKET}/{key}"
    )
    return count


if __name__ == "__main__":
    main()
