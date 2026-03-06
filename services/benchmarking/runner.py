from __future__ import annotations

import logging
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

import dask
import pandas as pd

from .config import BenchmarkConfig, parse_config
from .measurements import run_profile_measurements
from .reporting import RESULT_HEADERS, RAW_HEADERS, aggregate_results, to_raw_csv_rows, write_csv, write_json
from .run_registry import RunContext, create_run_context
from .sources import build_source_profiles



def _configure_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("services.benchmarking")
    logger.setLevel(logging.INFO)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger



def _detect_git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    commit = completed.stdout.strip()
    return commit if commit else None



def make_run_info(cfg: BenchmarkConfig, run_ctx: RunContext) -> dict[str, Any]:
    return {
        "run_index": run_ctx.run_index,
        "run_datetime": run_ctx.run_datetime,
        "execution_mode": cfg.execution_mode,
        "git_commit": _detect_git_commit(),
        "python_version": platform.python_version(),
        "dask_version": dask.__version__,
        "pandas_version": pd.__version__,
    }



def run_benchmark(cfg: BenchmarkConfig) -> Path:
    run_ctx = create_run_context(cfg.runs_root)
    logger = _configure_logger(run_ctx.logs_dir / "runner.log")
    logger.info("Benchmark run directory created: %s", run_ctx.run_dir)

    run_info = make_run_info(cfg, run_ctx)
    write_json(run_ctx.run_dir / "run_info.json", run_info)
    write_json(run_ctx.run_dir / "config_effective.json", cfg.to_dict())

    raw_measurements: list[dict[str, Any]] = []
    profiles = build_source_profiles(cfg)
    for profile in profiles:
        logger.info("Running profile: %s", profile.name)
        profile_rows = run_profile_measurements(
            profile,
            cfg,
            logger=logger,
            artifacts_dir=run_ctx.artifacts_dir,
        )
        raw_measurements.extend(profile_rows)

    write_csv(run_ctx.run_dir / "raw_measurements.csv", to_raw_csv_rows(raw_measurements), RAW_HEADERS)
    results = aggregate_results(raw_measurements, [profile.name for profile in profiles])
    write_csv(run_ctx.run_dir / "results.csv", results, RESULT_HEADERS)

    logger.info("Benchmark completed. Results: %s", run_ctx.run_dir / "results.csv")
    return run_ctx.run_dir



def main(argv: list[str] | None = None) -> int:
    cfg = parse_config(argv)
    run_benchmark(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
