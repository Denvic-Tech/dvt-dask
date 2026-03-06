from __future__ import annotations

import re

from services.benchmarking.run_registry import create_run_context



def test_run_context_index_and_naming(tmp_path):
    runs_root = tmp_path / "runs"

    first = create_run_context(runs_root)
    second = create_run_context(runs_root)

    assert first.run_index == 0
    assert second.run_index == 1
    assert re.match(r"^\d{4}_\d{8}_\d{6}$", first.run_dir.name)
    assert re.match(r"^\d{4}_\d{8}_\d{6}$", second.run_dir.name)
    assert first.logs_dir.exists()
    assert first.artifacts_dir.exists()
