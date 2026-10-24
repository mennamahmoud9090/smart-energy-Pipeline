import json
from pathlib import Path

import pandas as pd
import pytest

from shared.contracts import CUSTOMER_PROFILES, REQUIRED_READING_COLUMNS
from shared.serialization import json_default


def test_required_reading_columns_match_raw_contract():
    assert REQUIRED_READING_COLUMNS == {
        "reading_id", "event_timestamp", "meter_id", "transformer_id",
        "voltage_v", "active_power_kw", "reactive_power_kvar", "energy_kwh",
        "power_factor", "schema_version",
    }


def test_customer_profiles_cover_all_documented_customer_types():
    assert set(CUSTOMER_PROFILES) == {
        "RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL", "AGRICULTURAL",
        "GOVERNMENT", "PUBLIC_LIGHTING", "OTHER",
    }
    assert all(len(profile) == 2 for profile in CUSTOMER_PROFILES.values())


def test_json_default_converts_pandas_timestamp():
    timestamp = pd.Timestamp("2026-07-01T18:15:00Z")
    assert json.loads(json.dumps({"timestamp": timestamp}, default=json_default))[
        "timestamp"
    ] == "2026-07-01T18:15:00+00:00"


def test_json_default_converts_numpy_scalar():
    import numpy as np

    assert json.loads(json.dumps({"value": np.int64(7)}, default=json_default)) == {
        "value": 7
    }


def test_json_default_rejects_unsupported_values():
    with pytest.raises(TypeError, match="Cannot serialize object"):
        json_default(object())


def test_upload_parquet_batch_uses_expected_key_and_cleans_temp_file(tmp_path, monkeypatch):
    from shared.parquet_upload import upload_parquet_batch

    uploaded = {}

    def fake_to_parquet(frame, path, index=False, engine=None):
        path.write_bytes(b"fake parquet bytes")

    def fake_upload(path, bucket, key):
        uploaded.update(path=path, bucket=bucket, key=key, exists=Path(path).exists())

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)
    class FakeS3:
        upload_file = staticmethod(fake_upload)

    count, key = upload_parquet_batch(
        FakeS3(), [{"reading_id": "r1", "event_timestamp": "2026-07-01T00:00:00Z"}],
        bucket="lakehouse",
        key_prefix="bronze/streaming/run_id=demo/",
        local_dir=tmp_path,
        filename_prefix="batch_",
        batch_number=3,
        index_width=6,
        random_key_suffix=False,
    )

    assert count == 1
    assert key == "bronze/streaming/run_id=demo/batch-000003.parquet"
    assert uploaded["bucket"] == "lakehouse"
    assert uploaded["key"] == key
    assert uploaded["exists"] is True
    assert list(tmp_path.iterdir()) == []
