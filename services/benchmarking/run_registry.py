from __future__ import annotations

import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

RUN_DIR_RE = re.compile(r"^(?P<index>\d{4})_\d{8}_\d{6}$")


@dataclass(frozen=True)
class RunContext:
    run_dir: Path
    logs_dir: Path
    artifacts_dir: Path
    run_index: int
    run_datetime: str


@contextmanager
def _run_index_lock(runs_root: Path, timeout_seconds: float = 10.0):
    lock_path = runs_root / ".run_index.lock"
    fd: int | None = None
    started = perf_counter()
    while fd is None:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode("utf-8"))
        except FileExistsError:
            if perf_counter() - started >= timeout_seconds:
                raise TimeoutError(f"Timed out waiting for run-index lock at {lock_path}")
            time.sleep(0.05)

    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass



def _next_run_index(runs_root: Path) -> int:
    max_index = -1
    for child in runs_root.iterdir():
        if not child.is_dir():
            continue
        match = RUN_DIR_RE.match(child.name)
        if match is None:
            continue
        max_index = max(max_index, int(match.group("index")))
    return max_index + 1



def create_run_context(runs_root: Path) -> RunContext:
    runs_root.mkdir(parents=True, exist_ok=True)
    with _run_index_lock(runs_root):
        run_index = _next_run_index(runs_root)
        run_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = runs_root / f"{run_index:04d}_{run_datetime}"
        run_dir.mkdir(parents=True, exist_ok=False)

    logs_dir = run_dir / "logs"
    artifacts_dir = run_dir / "artifacts"
    logs_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    return RunContext(
        run_dir=run_dir,
        logs_dir=logs_dir,
        artifacts_dir=artifacts_dir,
        run_index=run_index,
        run_datetime=run_datetime,
    )
