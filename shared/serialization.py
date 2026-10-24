"""Shared JSON serialization helpers."""
import pandas as pd


def json_default(value):
    """Convert pandas/NumPy scalar values into JSON-compatible values."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Cannot serialize {type(value).__name__}")
