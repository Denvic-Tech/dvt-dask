from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable

import dask.dataframe as dd
import pandas as pd
from dask import delayed

from .config import BASE_SOURCE_PROFILES, BenchmarkConfig, REQUIRED_SOURCE_PROFILES
from .generators import generate_synthetic_dataframe


@dataclass(frozen=True)
class SourceProfile:
    name: str
    expected_known_divisions: bool
    make_dataframe: Callable[[int], dd.DataFrame]
    with_callbacks: bool = False



def _split_partitions(frame: pd.DataFrame, partition_size: int) -> list[pd.DataFrame]:
    partitions: list[pd.DataFrame] = []
    for start in range(0, len(frame), partition_size):
        part = frame.iloc[start : start + partition_size].copy()
        part.index = pd.RangeIndex(start=start, stop=start + len(part), step=1)
        partitions.append(part)
    return partitions


@delayed
def _load_partition(partition: pd.DataFrame) -> pd.DataFrame:
    return partition



def _known_divisions_from_partitions(partitions: list[pd.DataFrame]) -> tuple[int, ...]:
    if not partitions:
        return (0, 0)
    starts = [int(part.index[0]) for part in partitions]
    end = int(partitions[-1].index[-1])
    return tuple(starts + [end])



def _build_from_pandas(rows: int, partition_size: int, seed: int, cfg: BenchmarkConfig) -> dd.DataFrame:
    frame = generate_synthetic_dataframe(rows, seed, null_fraction=cfg.null_fraction, skew_factor=cfg.skew_factor)
    npartitions = max(1, math.ceil(rows / partition_size))
    return dd.from_pandas(frame, npartitions=npartitions, sort=True)



def _build_from_delayed(rows: int, partition_size: int, seed: int, cfg: BenchmarkConfig, known: bool) -> dd.DataFrame:
    frame = generate_synthetic_dataframe(rows, seed, null_fraction=cfg.null_fraction, skew_factor=cfg.skew_factor)
    partitions = _split_partitions(frame, partition_size)
    delayed_partitions = [_load_partition(partition) for partition in partitions]
    meta = frame.iloc[0:0]
    if known:
        divisions: tuple[int | None, ...] = _known_divisions_from_partitions(partitions)
    else:
        divisions = tuple(None for _ in range(len(partitions) + 1))
    return dd.from_delayed(delayed_partitions, meta=meta, divisions=divisions)



def _validate_divisions(ddf: dd.DataFrame, expected_known_divisions: bool, profile_name: str) -> None:
    if ddf.known_divisions != expected_known_divisions:
        raise ValueError(
            f"Profile {profile_name} expected known_divisions={expected_known_divisions}, got {ddf.known_divisions}"
        )



def _build_base_profiles(cfg: BenchmarkConfig) -> list[SourceProfile]:
    return [
        SourceProfile(
            name="from_pandas_known_normal",
            expected_known_divisions=True,
            make_dataframe=lambda seed: _build_from_pandas(
                cfg.rows_normal,
                cfg.normal_partition_size,
                seed,
                cfg,
            ),
        ),
        SourceProfile(
            name="delayed_known_normal",
            expected_known_divisions=True,
            make_dataframe=lambda seed: _build_from_delayed(
                cfg.rows_normal,
                cfg.normal_partition_size,
                seed,
                cfg,
                known=True,
            ),
        ),
        SourceProfile(
            name="delayed_unknown_normal",
            expected_known_divisions=False,
            make_dataframe=lambda seed: _build_from_delayed(
                cfg.rows_normal,
                cfg.normal_partition_size,
                seed,
                cfg,
                known=False,
            ),
        ),
        SourceProfile(
            name="from_pandas_known_large_partitions",
            expected_known_divisions=True,
            make_dataframe=lambda seed: _build_from_pandas(
                cfg.rows_large_partition,
                cfg.large_partition_size,
                seed,
                cfg,
            ),
        ),
        SourceProfile(
            name="from_pandas_known_small_partitions",
            expected_known_divisions=True,
            make_dataframe=lambda seed: _build_from_pandas(
                cfg.rows_small_partition,
                cfg.small_partition_size,
                seed,
                cfg,
            ),
        ),
    ]



def build_source_profiles(cfg: BenchmarkConfig) -> list[SourceProfile]:
    base_profiles = _build_base_profiles(cfg)
    base_names = tuple(profile.name for profile in base_profiles)
    if base_names != BASE_SOURCE_PROFILES:
        raise ValueError(f"Profiles order mismatch: expected {BASE_SOURCE_PROFILES}, got {base_names}")

    profiles: list[SourceProfile] = []
    for base_profile in base_profiles:
        profiles.append(base_profile)
        profiles.append(
            SourceProfile(
                name=f"{base_profile.name}_with_cb",
                expected_known_divisions=base_profile.expected_known_divisions,
                make_dataframe=base_profile.make_dataframe,
                with_callbacks=True,
            )
        )

    names = tuple(profile.name for profile in profiles)
    if names != REQUIRED_SOURCE_PROFILES:
        raise ValueError(f"Profiles order mismatch: expected {REQUIRED_SOURCE_PROFILES}, got {names}")

    seed_for_validation = cfg.seed
    for profile in profiles:
        ddf = profile.make_dataframe(seed_for_validation)
        _validate_divisions(ddf, profile.expected_known_divisions, profile.name)

    return profiles
