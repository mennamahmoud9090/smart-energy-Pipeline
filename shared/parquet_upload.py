"""Shared helper for uploading record batches as temporary Parquet objects."""
from pathlib import Path
import uuid

import pandas as pd


def upload_parquet_batch(
    s3,
    records,
    *,
    bucket,
    key_prefix,
    local_dir,
    filename_prefix,
    batch_number,
    index_width=5,
    random_key_suffix=True,
):
    """Serialize records to a temporary Parquet file and upload it.

    Returns ``(row_count, object_key)``. Temporary local files are removed even
    when serialization or upload fails.
    """
    frame = pd.DataFrame(records)
    if "event_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(
            frame["event_timestamp"], utc=True, errors="raise"
        )

    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / f"{filename_prefix}{uuid.uuid4().hex}.parquet"

    suffix = f"-{uuid.uuid4().hex}" if random_key_suffix else ""
    key = f"{key_prefix}batch-{batch_number:0{index_width}d}{suffix}.parquet"

    try:
        frame.to_parquet(local_path, index=False, engine="pyarrow")
        s3.upload_file(str(local_path), bucket, key)
        return len(records), key
    finally:
        local_path.unlink(missing_ok=True)
