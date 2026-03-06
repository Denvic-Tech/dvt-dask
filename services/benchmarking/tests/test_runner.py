from __future__ import annotations

import csv
import json
import math

import pytest

from services.benchmarking.config import BenchmarkConfig, REQUIRED_SOURCE_PROFILES
from services.benchmarking.runner import run_benchmark



def _small_config(tmp_path, execution_mode: str) -> BenchmarkConfig:
    return BenchmarkConfig(
        repeats=1,
        seed=7,
        scheduler="threads",
        rows_normal=80,
        rows_large_partition=90,
        rows_small_partition=60,
        normal_partition_size=30,
        large_partition_size=90,
        small_partition_size=10,
        null_fraction=0.05,
        skew_factor=1.3,
        memory_sample_interval_ms=20,
        graph_artifacts=False,
        execution_mode=execution_mode,
        runs_root=tmp_path / "runs",
    ).validate()



def test_runner_smoke_writes_required_artifacts(tmp_path):
    cfg = _small_config(tmp_path, execution_mode="host")

    run_dir = run_benchmark(cfg)

    assert (run_dir / "run_info.json").exists()
    assert (run_dir / "config_effective.json").exists()
    assert (run_dir / "results.csv").exists()
    assert (run_dir / "raw_measurements.csv").exists()
    assert (run_dir / "logs" / "runner.log").exists()



def test_results_csv_has_required_columns_and_profiles(tmp_path):
    cfg = _small_config(tmp_path, execution_mode="host")

    run_dir = run_benchmark(cfg)
    results_path = run_dir / "results.csv"

    with results_path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)

    assert reader.fieldnames == [
        "тип источника",
        "мин. время",
        "макс. время",
        "среднее время",
        "мин. потребление памяти",
        "макс. потребление памяти",
        "сред. потребление памяти",
    ]
    assert {row["тип источника"] for row in rows} == set(REQUIRED_SOURCE_PROFILES)



def test_smoke_run_has_successful_measurements_for_all_profiles(tmp_path):
    cfg = BenchmarkConfig(
        repeats=2,
        seed=7,
        scheduler="threads",
        rows_normal=120,
        rows_large_partition=140,
        rows_small_partition=100,
        normal_partition_size=30,
        large_partition_size=140,
        small_partition_size=10,
        null_fraction=0.05,
        skew_factor=1.3,
        memory_sample_interval_ms=20,
        graph_artifacts=False,
        execution_mode="host",
        runs_root=tmp_path / "runs",
    ).validate()

    run_dir = run_benchmark(cfg)
    raw_path = run_dir / "raw_measurements.csv"
    results_path = run_dir / "results.csv"

    with raw_path.open("r", encoding="utf-8", newline="") as stream:
        raw_rows = list(csv.DictReader(stream))
    assert raw_rows
    assert all(row["успех"] == "True" for row in raw_rows)

    with results_path.open("r", encoding="utf-8", newline="") as stream:
        result_rows = list(csv.DictReader(stream))
    assert result_rows
    for row in result_rows:
        assert not math.isnan(float(row["среднее время"]))
        assert not math.isnan(float(row["сред. потребление памяти"]))


@pytest.mark.parametrize("execution_mode", ["host", "docker"])
def test_run_info_contains_execution_mode_marker(tmp_path, execution_mode):
    cfg = _small_config(tmp_path, execution_mode=execution_mode)

    run_dir = run_benchmark(cfg)
    info = json.loads((run_dir / "run_info.json").read_text(encoding="utf-8"))

    assert info["execution_mode"] == execution_mode
