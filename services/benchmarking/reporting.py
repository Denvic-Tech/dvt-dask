from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean
from typing import Any

RAW_HEADERS = [
    "тип источника",
    "итерация",
    "время_сек",
    "память_min_mib",
    "память_max_mib",
    "память_avg_mib",
    "успех",
    "ошибка",
]

RESULT_HEADERS = [
    "тип источника",
    "мин. время",
    "макс. время",
    "среднее время",
    "мин. потребление памяти",
    "макс. потребление памяти",
    "сред. потребление памяти",
]



def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def to_raw_csv_rows(measurements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in measurements:
        rows.append(
            {
                "тип источника": item["source_type"],
                "итерация": item["iteration"],
                "время_сек": item["time_sec"],
                "память_min_mib": item["memory_min_mib"],
                "память_max_mib": item["memory_max_mib"],
                "память_avg_mib": item["memory_avg_mib"],
                "успех": item["success"],
                "ошибка": item["error"],
            }
        )
    return rows



def write_csv(path: Path, rows: list[dict[str, Any]], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)



def aggregate_results(measurements: list[dict[str, Any]], profile_order: list[str]) -> list[dict[str, Any]]:
    aggregated: list[dict[str, Any]] = []
    for profile in profile_order:
        ok_rows = [item for item in measurements if item["source_type"] == profile and item["success"]]
        if not ok_rows:
            aggregated.append(
                {
                    "тип источника": profile,
                    "мин. время": float("nan"),
                    "макс. время": float("nan"),
                    "среднее время": float("nan"),
                    "мин. потребление памяти": float("nan"),
                    "макс. потребление памяти": float("nan"),
                    "сред. потребление памяти": float("nan"),
                }
            )
            continue

        times = [float(item["time_sec"]) for item in ok_rows]
        memory = [float(item["memory_max_mib"]) for item in ok_rows]
        aggregated.append(
            {
                "тип источника": profile,
                "мин. время": min(times),
                "макс. время": max(times),
                "среднее время": mean(times),
                "мин. потребление памяти": min(memory),
                "макс. потребление памяти": max(memory),
                "сред. потребление памяти": mean(memory),
            }
        )

    return aggregated
