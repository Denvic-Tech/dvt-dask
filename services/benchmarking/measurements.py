from __future__ import annotations

import gc
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from time import perf_counter
from typing import Any

import dask

from .config import BenchmarkConfig
from .pipeline import build_heavy_pipeline, save_pipeline_graph
from .sources import SourceProfile

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


class ProcessMemorySampler:
    def __init__(self, interval_seconds: float) -> None:
        if psutil is None:
            raise RuntimeError("psutil is required for memory measurements")
        self._interval_seconds = interval_seconds
        self._samples_mib: list[float] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = psutil.Process(os.getpid())

    def _sample_once(self) -> None:
        rss_bytes = self._process.memory_info().rss
        mib = rss_bytes / (1024 * 1024)
        with self._lock:
            self._samples_mib.append(mib)

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            self._sample_once()

    def start(self) -> None:
        self._stop_event.clear()
        self._sample_once()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, float]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._sample_once()
        with self._lock:
            samples = list(self._samples_mib)
        if not samples:
            samples = [0.0]
        return {
            "memory_min_mib": min(samples),
            "memory_max_mib": max(samples),
            "memory_avg_mib": mean(samples),
        }



def _attach_callbacks_for_profile(
    profile: SourceProfile,
    iteration: int,
    artifacts_dir: Path,
    logger: Any,
    pipeline,
):
    callbacks_dir = artifacts_dir / "callbacks"
    callbacks_dir.mkdir(parents=True, exist_ok=True)

    lock = threading.Lock()
    stats: dict[str, Any] = {
        "profile": profile.name,
        "iteration": iteration,
        "operation_id": f"{profile.name}_iter_{iteration:02d}",
        "start_calls": 0,
        "end_calls": 0,
        "error_calls": 0,
        "partition_calls": 0,
        "partition_finish_calls": 0,
        "partition_count_values": [],
        "partition_numbers_seen": [],
        "stages_seen": [],
        "errors": [],
    }

    def on_start(ddf_meta, operation_id, profile_name, iter_no):
        with lock:
            stats["start_calls"] += 1
            stats["columns_at_start"] = list(getattr(ddf_meta, "columns", []))
            stats["operation_id_seen"] = operation_id
            stats["profile_from_metadata"] = profile_name
            stats["iteration_from_metadata"] = iter_no

    def on_end(ddf_meta, operation_id, profile_name, iter_no):
        with lock:
            stats["end_calls"] += 1
            stats["columns_at_end"] = list(getattr(ddf_meta, "columns", []))
            stats["operation_id_seen"] = operation_id
            stats["profile_from_metadata"] = profile_name
            stats["iteration_from_metadata"] = iter_no

    def on_partition(_partition, operation_id, profile_name, iter_no, partition_info):
        with lock:
            stats["partition_calls"] += 1
            stage = str(partition_info.get("stage"))
            if stage == "finish":
                stats["partition_finish_calls"] += 1
            stats["stages_seen"].append(stage)
            stats["partition_numbers_seen"].append(int(partition_info.get("number", -1)))
            stats["partition_count_values"].append(int(partition_info.get("partition_count", -1)))
            stats["operation_id_seen"] = operation_id
            stats["profile_from_metadata"] = profile_name
            stats["iteration_from_metadata"] = iter_no

    def on_error(ddf_meta, operation_id, exc, profile_name, iter_no):
        with lock:
            stats["error_calls"] += 1
            stats["errors"].append(
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "columns": list(getattr(ddf_meta, "columns", [])),
                }
            )
            stats["operation_id_seen"] = operation_id
            stats["profile_from_metadata"] = profile_name
            stats["iteration_from_metadata"] = iter_no

    pipeline = pipeline.add_callbacks(
        on_start=on_start,
        on_end=on_end,
        on_partition=on_partition,
        on_error=on_error,
        operation_id=stats["operation_id"],
        operation_type="benchmark_pipeline",
        metadata={"profile_name": profile.name, "iter_no": iteration},
    )

    artifact_path = callbacks_dir / f"{profile.name}_iter_{iteration:02d}.json"

    def finalize(success: bool, error: str) -> None:
        with lock:
            normalized = {
                **stats,
                "success": success,
                "error": error,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "unique_partitions_seen": sorted({n for n in stats["partition_numbers_seen"] if n >= 0}),
                "unique_partition_count_values": sorted({n for n in stats["partition_count_values"] if n >= 0}),
                "unique_stages": sorted(set(stats["stages_seen"])),
            }
        artifact_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved callbacks artifact: %s", artifact_path)

    return pipeline, finalize



def run_profile_measurements(
    profile: SourceProfile,
    cfg: BenchmarkConfig,
    *,
    logger: Any,
    artifacts_dir: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    interval_seconds = cfg.memory_sample_interval_ms / 1000.0

    dask_tmp_dir = artifacts_dir / "dask_tmp"
    dask_tmp_dir.mkdir(parents=True, exist_ok=True)

    for iteration in range(1, cfg.repeats + 1):
        iteration_seed = cfg.seed + iteration - 1
        logger.info("profile=%s iteration=%s seed=%s", profile.name, iteration, iteration_seed)

        sampler = ProcessMemorySampler(interval_seconds)
        elapsed = 0.0
        success = True
        error = ""
        callback_finalize = None

        try:
            ddf = profile.make_dataframe(iteration_seed)
            pipeline = build_heavy_pipeline(ddf, cfg)

            if profile.with_callbacks:
                pipeline, callback_finalize = _attach_callbacks_for_profile(
                    profile=profile,
                    iteration=iteration,
                    artifacts_dir=artifacts_dir,
                    logger=logger,
                    pipeline=pipeline,
                )

            if cfg.graph_artifacts and iteration == 1:
                graph_path = artifacts_dir / f"graph_{profile.name}.svg"
                graph_saved = save_pipeline_graph(pipeline, graph_path)
                if graph_saved:
                    logger.info("Saved graph artifact: %s", graph_path)
                else:
                    logger.warning("Failed to save graph artifact for profile=%s", profile.name)

            sampler.start()
            start = perf_counter()
            with dask.config.set(scheduler=cfg.scheduler, temporary_directory=str(dask_tmp_dir)):
                _ = pipeline.compute()
            elapsed = perf_counter() - start
        except Exception as exc:  # pragma: no cover - exercised in failure paths
            success = False
            error = f"{type(exc).__name__}: {exc}"
            logger.exception("profile=%s iteration=%s failed", profile.name, iteration)
        finally:
            gc.collect()
            memory_stats = sampler.stop()
            if callback_finalize is not None:
                try:
                    callback_finalize(success, error)
                except Exception:  # pragma: no cover
                    logger.exception("Failed to save callbacks artifact for profile=%s iteration=%s", profile.name, iteration)

        rows.append(
            {
                "source_type": profile.name,
                "iteration": iteration,
                "time_sec": elapsed,
                "memory_min_mib": memory_stats["memory_min_mib"],
                "memory_max_mib": memory_stats["memory_max_mib"],
                "memory_avg_mib": memory_stats["memory_avg_mib"],
                "success": success,
                "error": error,
            }
        )

    return rows
