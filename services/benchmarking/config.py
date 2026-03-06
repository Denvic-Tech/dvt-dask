from __future__ import annotations

import argparse
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

BASE_SOURCE_PROFILES: tuple[str, ...] = (
    "from_pandas_known_normal",
    "delayed_known_normal",
    "delayed_unknown_normal",
    "from_pandas_known_large_partitions",
    "from_pandas_known_small_partitions",
)

REQUIRED_SOURCE_PROFILES: tuple[str, ...] = tuple(
    profile_name
    for base_name in BASE_SOURCE_PROFILES
    for profile_name in (base_name, f"{base_name}_with_cb")
)


@dataclass(frozen=True)
class BenchmarkConfig:
    repeats: int = 10
    seed: int = 42
    scheduler: str = "threads"
    rows_normal: int = 300_000
    rows_large_partition: int = 1_000_000
    rows_small_partition: int = 300_000
    normal_partition_size: int = 10_000
    large_partition_size: int = 200_000
    small_partition_size: int = 100
    null_fraction: float = 0.05
    skew_factor: float = 1.4
    memory_sample_interval_ms: int = 50
    graph_artifacts: bool = False
    execution_mode: str = "host"
    runs_root: Path = Path("services/benchmarking/runs")

    def validate(self) -> "BenchmarkConfig":
        if self.repeats < 1:
            raise ValueError("repeats must be >= 1")
        if self.rows_normal < 1 or self.rows_large_partition < 1 or self.rows_small_partition < 1:
            raise ValueError("row counts must be >= 1")
        if min(self.normal_partition_size, self.large_partition_size, self.small_partition_size) < 1:
            raise ValueError("partition sizes must be >= 1")
        if not 0 <= self.null_fraction < 1:
            raise ValueError("null_fraction must be in [0, 1)")
        if self.memory_sample_interval_ms < 10:
            raise ValueError("memory_sample_interval_ms must be >= 10")
        if self.scheduler not in {"threads", "processes", "single-threaded"}:
            raise ValueError("scheduler must be threads|processes|single-threaded")
        if self.execution_mode not in {"host", "docker"}:
            raise ValueError("execution_mode must be host|docker")
        return self

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["runs_root"] = str(self.runs_root)
        payload["required_source_profiles"] = list(REQUIRED_SOURCE_PROFILES)
        return payload



def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Dask DataFrame benchmarking service")
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scheduler", choices=["threads", "processes", "single-threaded"], default="threads")
    parser.add_argument("--rows-normal", type=int, default=300_000)
    parser.add_argument("--rows-large-partition", type=int, default=1_000_000)
    parser.add_argument("--rows-small-partition", type=int, default=300_000)
    parser.add_argument("--normal-partition-size", type=int, default=10_000)
    parser.add_argument("--large-partition-size", type=int, default=200_000)
    parser.add_argument("--small-partition-size", type=int, default=100)
    parser.add_argument("--null-fraction", type=float, default=0.05)
    parser.add_argument("--skew-factor", type=float, default=1.4)
    parser.add_argument("--memory-sample-interval-ms", type=int, default=50)
    parser.add_argument("--graph-artifacts", action="store_true")
    parser.add_argument(
        "--execution-mode",
        choices=["host", "docker"],
        default=os.environ.get("BENCHMARK_EXECUTION_MODE", "host"),
    )
    parser.add_argument("--runs-root", type=Path, default=Path("services/benchmarking/runs"))
    return parser



def parse_config(argv: list[str] | None = None) -> BenchmarkConfig:
    args = build_arg_parser().parse_args(argv)
    cfg = BenchmarkConfig(
        repeats=args.repeats,
        seed=args.seed,
        scheduler=args.scheduler,
        rows_normal=args.rows_normal,
        rows_large_partition=args.rows_large_partition,
        rows_small_partition=args.rows_small_partition,
        normal_partition_size=args.normal_partition_size,
        large_partition_size=args.large_partition_size,
        small_partition_size=args.small_partition_size,
        null_fraction=args.null_fraction,
        skew_factor=args.skew_factor,
        memory_sample_interval_ms=args.memory_sample_interval_ms,
        graph_artifacts=args.graph_artifacts,
        execution_mode=args.execution_mode,
        runs_root=args.runs_root,
    )
    return cfg.validate()
