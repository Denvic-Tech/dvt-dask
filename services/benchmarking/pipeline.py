from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

from .config import BenchmarkConfig
from .generators import build_lookup_table



def _partition_enrichment(partition: pd.DataFrame) -> pd.DataFrame:
    out = partition.copy()
    out["weighted_value"] = out["amount_norm"] * (out["quantity"].astype("float64") + 1.0)
    out["bucket"] = (out["lookup_key"] % 64).astype("int16")
    out["score_filled"] = out["score"].fillna(0.0)
    out["qty_log"] = np.log1p(out["quantity"].astype("float64"))
    out["event_hour"] = out["event_ts"].dt.hour.astype("int16")
    out["category_norm"] = out["category"].astype("string").fillna("missing")
    return out



def _partition_quality(partition: pd.DataFrame) -> pd.DataFrame:
    out = partition.copy()
    out["text_prefix"] = out["text"].fillna("").str.slice(0, 3)
    out["is_high_amount"] = (out["amount_norm"] > 2.0).astype("int8")
    out["weighted_value_sq"] = out["weighted_value"] * out["weighted_value"]
    out["score_centered"] = out["score_filled"] - out["score_filled"].mean()
    out["bucket_even"] = (out["bucket"] % 2 == 0).astype("int8")
    return out



def _merge_lookup_partition(partition: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    return partition.merge(lookup, on="lookup_key", how="left")



def _post_merge_partition(partition: pd.DataFrame) -> pd.DataFrame:
    out = partition.copy()
    out["tier"] = out["tier"].fillna("unknown")
    out["priority"] = out["priority"].fillna(-1).astype("int16")
    tier_weight = {"gold": 1.3, "silver": 1.1, "bronze": 1.0, "unknown": 0.9}
    out["tier_weight"] = out["tier"].map(tier_weight).fillna(0.9).astype("float64")
    out["priority_scaled"] = out["priority"].astype("float64") / 10.0
    out["weighted_tier_value"] = out["weighted_sum"] * out["tier_weight"]
    return out



def _rolling_partition(partition: pd.DataFrame) -> pd.DataFrame:
    if partition.empty:
        out = partition.copy()
        out["rolling_weighted_5"] = pd.Series(dtype="float64")
        out["rolling_rows_5"] = pd.Series(dtype="float64")
        out["rolling_score_3"] = pd.Series(dtype="float64")
        out["rolling_density_4"] = pd.Series(dtype="float64")
        return out

    out = partition.sort_values(["lookup_key", "event_day", "bucket"]).copy()
    grouped = out.groupby("lookup_key", sort=False, observed=False)
    out["rolling_weighted_5"] = grouped["weighted_sum"].transform(lambda s: s.rolling(window=5, min_periods=1).mean())
    out["rolling_rows_5"] = grouped["row_count"].transform(lambda s: s.rolling(window=5, min_periods=1).sum())
    out["rolling_score_3"] = grouped["score_mean"].transform(lambda s: s.rolling(window=3, min_periods=1).mean())
    out["rolling_density_4"] = grouped["weighted_density"].transform(lambda s: s.rolling(window=4, min_periods=1).mean())
    return out



def _stability_partition(partition: pd.DataFrame) -> pd.DataFrame:
    if partition.empty:
        out = partition.copy()
        out["bucket_rolling_weighted_3"] = pd.Series(dtype="float64")
        out["bucket_rolling_rows_3"] = pd.Series(dtype="float64")
        return out

    out = partition.sort_values(["lookup_key", "event_day", "bucket"]).copy()
    grouped = out.groupby(["lookup_key", "bucket"], sort=False, observed=False)
    out["bucket_rolling_weighted_3"] = grouped["weighted_sum"].transform(
        lambda s: s.rolling(window=3, min_periods=1).mean()
    )
    out["bucket_rolling_rows_3"] = grouped["row_count"].transform(lambda s: s.rolling(window=3, min_periods=1).sum())
    return out



def build_heavy_pipeline(ddf: dd.DataFrame, cfg: BenchmarkConfig) -> dd.DataFrame:
    filtered = ddf[
        (ddf["quantity"] > 1)
        & (ddf["amount"].fillna(0.0) > 0.0)
        & (ddf["category"].isin(["A", "B", "C"]))
        & (ddf["event_ts"].notnull())
    ]

    filtered = filtered[(filtered["region"].notnull()) & (filtered["text"].fillna("").str.len() >= 0)]

    enriched = filtered.assign(
        amount_norm=filtered["amount"].fillna(0.0) / (filtered["quantity"] + 1),
        event_day=filtered["event_ts"].dt.floor("D"),
        text_len=filtered["text"].fillna("").str.len().astype("int64"),
    )

    enriched = enriched.assign(
        amount_scaled=enriched["amount_norm"].clip(lower=0.0, upper=10_000.0),
        quantity_x_text=enriched["quantity"].astype("float64") * (enriched["text_len"] + 1),
        score_bucket=(enriched["score"].fillna(0.0) // 100).astype("int16"),
    )

    mapped = enriched.map_partitions(_partition_enrichment, meta=_partition_enrichment(enriched._meta))
    mapped = mapped.map_partitions(_partition_quality, meta=_partition_quality(mapped._meta))

    grouped = (
        mapped.groupby(["lookup_key", "event_day", "bucket"])
        .agg(
            {
                "weighted_value": "sum",
                "weighted_value_sq": "sum",
                "row_id": "count",
                "text_len": "mean",
                "score_filled": "mean",
                "qty_log": "mean",
                "is_high_amount": "sum",
                "bucket_even": "sum",
            }
        )
        .rename(
            columns={
                "weighted_value": "weighted_sum",
                "weighted_value_sq": "weighted_sq_sum",
                "row_id": "row_count",
                "text_len": "mean_text_len",
                "score_filled": "score_mean",
                "qty_log": "qty_log_mean",
                "is_high_amount": "high_amount_count",
                "bucket_even": "bucket_even_count",
            }
        )
        .reset_index()
    )

    grouped = grouped.assign(
        weighted_density=grouped["weighted_sum"] / (grouped["row_count"] + 1.0),
        high_amount_ratio=grouped["high_amount_count"] / (grouped["row_count"] + 1.0),
        bucket_even_ratio=grouped["bucket_even_count"] / (grouped["row_count"] + 1.0),
        score_pressure=grouped["score_mean"] / (grouped["qty_log_mean"] + 1.0),
    )

    lookup = build_lookup_table()
    merge_meta = grouped._meta.merge(lookup.iloc[0:0], on="lookup_key", how="left")
    merged = grouped.map_partitions(_merge_lookup_partition, lookup=lookup, meta=merge_meta)
    merged = merged.map_partitions(_post_merge_partition, meta=_post_merge_partition(merged._meta))

    shuffled_primary = merged.shuffle("lookup_key", shuffle_method="tasks")
    rolled = shuffled_primary.map_partitions(_rolling_partition, meta=_rolling_partition(shuffled_primary._meta))

    shuffled_secondary = rolled.shuffle("bucket", shuffle_method="tasks")
    stabilized = shuffled_secondary.map_partitions(_stability_partition, meta=_stability_partition(shuffled_secondary._meta))

    stabilized = stabilized.assign(
        blended_score=(stabilized["rolling_score_3"] + stabilized["score_pressure"]) / 2.0,
        composite_pressure=stabilized["weighted_tier_value"] / (stabilized["rolling_rows_5"] + 1.0),
    )

    deduped = stabilized.drop_duplicates(subset=["lookup_key", "event_day", "bucket"], keep="last")
    final = deduped.drop_duplicates(subset=["lookup_key", "event_day"], keep="last")
    final = final[final["row_count"] > 0]
    return final



def save_pipeline_graph(collection: dd.DataFrame, output_path: Path) -> bool:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        collection.visualize(filename=str(output_path), format=output_path.suffix.lstrip("."))
        return True
    except Exception:
        return False
