from __future__ import annotations

import numpy as np
import pandas as pd


CATEGORY_VALUES = ["A", "B", "C", "D", "E"]
REGION_VALUES = ["north", "south", "east", "west", "central"]
TEXT_VALUES = ["alpha", "beta", "gamma", "delta", "omega", "sigma"]



def _inject_nulls(series: pd.Series, fraction: float, rng: np.random.Generator, null_value: object = np.nan) -> pd.Series:
    if fraction <= 0:
        return series
    mask = rng.random(len(series)) < fraction
    out = series.copy()
    out.loc[mask] = null_value
    return out



def generate_synthetic_dataframe(
    rows: int,
    seed: int,
    *,
    null_fraction: float,
    skew_factor: float,
) -> pd.DataFrame:
    """Generate reproducible mixed-type data with skew and nulls."""
    rng = np.random.default_rng(seed)
    row_id = np.arange(rows, dtype=np.int64)

    # Long-tail key distribution to stress groupby/merge and shuffles.
    zipf_raw = rng.zipf(a=max(skew_factor, 1.01), size=rows)
    high_cardinality = max(128, rows // 10)
    customer_key = (zipf_raw % high_cardinality).astype(np.int64)
    lookup_key = (customer_key % 2048).astype(np.int64)

    quantity = rng.integers(1, 128, size=rows, dtype=np.int64)
    amount = rng.normal(loc=100.0, scale=35.0, size=rows).astype(np.float64)
    score = rng.uniform(low=0.0, high=1_000.0, size=rows).astype(np.float64)

    categories = pd.Categorical(
        rng.choice(CATEGORY_VALUES, size=rows, p=[0.45, 0.22, 0.16, 0.10, 0.07]),
        categories=CATEGORY_VALUES,
    )
    region = pd.Series(rng.choice(REGION_VALUES, size=rows), dtype="string")
    text = pd.Series(rng.choice(TEXT_VALUES, size=rows), dtype="string")

    base_ts = np.datetime64("2025-01-01T00:00:00")
    minute_offsets = rng.integers(0, 180 * 24 * 60, size=rows, dtype=np.int64)
    event_ts = pd.to_datetime(base_ts + minute_offsets.astype("timedelta64[m]"))

    frame = pd.DataFrame(
        {
            "row_id": row_id,
            "customer_key": customer_key,
            "lookup_key": lookup_key,
            "quantity": quantity,
            "amount": amount,
            "score": score,
            "category": categories,
            "region": region,
            "text": text,
            "event_ts": event_ts,
        }
    )

    frame["amount"] = _inject_nulls(frame["amount"], null_fraction, rng)
    frame["score"] = _inject_nulls(frame["score"], null_fraction, rng)
    frame["category"] = _inject_nulls(frame["category"], null_fraction / 2.0, rng)
    frame["text"] = _inject_nulls(frame["text"], null_fraction, rng, null_value=pd.NA).astype("string")
    frame["event_ts"] = _inject_nulls(frame["event_ts"], null_fraction / 4.0, rng, null_value=pd.NaT)

    frame.index = pd.RangeIndex(start=0, stop=rows, step=1)
    return frame



def build_lookup_table(size: int = 2048) -> pd.DataFrame:
    key = np.arange(size, dtype=np.int64)
    tiers = np.where(key % 7 == 0, "gold", np.where(key % 3 == 0, "silver", "bronze"))
    return pd.DataFrame(
        {
            "lookup_key": key,
            "tier": pd.Series(tiers, dtype="string"),
            "priority": (key % 11).astype(np.int16),
        }
    )
