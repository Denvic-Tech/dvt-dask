from __future__ import annotations

import asyncio
import io
import json
import os
import re
import subprocess
import threading
import traceback
from time import monotonic
import urllib.error
import urllib.parse
import urllib.request
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Literal
from uuid import UUID

from services.project_mcp.runtime import InterpreterResolution


@dataclass
class ProjectMcpContext:
    project_root: Path
    env_loaded_keys: dict[str, str]
    interpreter: InterpreterResolution
    inline_sessions: dict[str, dict[str, Any]] = field(default_factory=dict)


def build_command_env(context: ProjectMcpContext, extra_env: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(context.project_root))
    if extra_env:
        env.update(extra_env)
    return env


def run_command(
    context: ProjectMcpContext,
    command: list[str],
    *,
    timeout_sec: int | None,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd or context.project_root),
            env=build_command_env(context, extra_env),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "success": completed.returncode == 0,
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "command": command,
            "cwd": str(cwd or context.project_root),
        }
    except subprocess.TimeoutExpired as exc:
        timeout_suffix = (
            f"\nTimed out after {timeout_sec} seconds" if timeout_sec is not None else "\nTimed out"
        )
        return {
            "success": False,
            "exit_code": None,
            "stdout": exc.stdout or "",
            "stderr": (exc.stderr or "") + timeout_suffix,
            "command": command,
            "cwd": str(cwd or context.project_root),
        }


def resolve_path_within_project(context: ProjectMcpContext, path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = context.project_root / path

    resolved = path.resolve()
    resolved.relative_to(context.project_root.resolve())
    return resolved


def get_runtime_info(
    context: ProjectMcpContext,
    *,
    verbose: bool = False,
) -> dict[str, Any]:

    return {
        "success": True,
        "project_root": str(context.project_root),
        "env_loaded_keys": sorted(context.env_loaded_keys.keys()),
        "interpreter": context.interpreter.to_dict(),
    }


def run_python_code(
    context: ProjectMcpContext,
    code: str,
    *,
    execution_mode: Literal["inline", "subprocess"] = "inline",
    session_id: str = "default",
    timeout_sec: int = 120,
) -> dict[str, Any]:
    if execution_mode == "subprocess":
        return run_command(
            context,
            [str(context.interpreter.python_executable), "-c", code],
            timeout_sec=timeout_sec,
        )

    namespace = context.inline_sessions.setdefault(session_id, {"__name__": "__main__"})
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    try:
        compiled = compile(code, "<project_mcp_inline>", "exec")
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exec(compiled, namespace, namespace)
        success = True
        error: str | None = None
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc(file=stderr_buffer)
        success = False
        error = f"{type(exc).__name__}: {exc}"

    return {
        "success": success,
        "exit_code": 0 if success else 1,
        "stdout": stdout_buffer.getvalue(),
        "stderr": stderr_buffer.getvalue(),
        "error": error,
        "session_id": session_id,
        "interpreter": str(context.interpreter.python_executable),
    }


def run_python_file(
    context: ProjectMcpContext,
    file_path: str,
    *,
    arguments: list[str] | None = None,
    timeout_sec: int = 300,
) -> dict[str, Any]:
    try:
        resolved_path = resolve_path_within_project(context, file_path)
    except ValueError:
        return {
            "success": False,
            "exit_code": 1,
            "stdout": "",
            "stderr": f"Path is outside project root: {file_path}",
            "command": [],
        }

    if not resolved_path.is_file():
        return {
            "success": False,
            "exit_code": 1,
            "stdout": "",
            "stderr": f"Python file not found: {resolved_path}",
            "command": [],
        }

    command = [str(context.interpreter.python_executable), str(resolved_path), *(arguments or [])]
    return run_command(context, command, timeout_sec=timeout_sec)


def list_inline_sessions(context: ProjectMcpContext) -> list[str]:
    return sorted(context.inline_sessions.keys())


def clear_inline_session(context: ProjectMcpContext, session_id: str | None = None) -> dict[str, Any]:
    if session_id is None:
        count = len(context.inline_sessions)
        context.inline_sessions.clear()
        return {"success": True, "cleared": count, "scope": "all"}

    existed = session_id in context.inline_sessions
    context.inline_sessions.pop(session_id, None)
    return {"success": True, "cleared": 1 if existed else 0, "scope": session_id}


def run_pytest(
    context: ProjectMcpContext,
    *,
    arguments: list[str] | None = None,
    timeout_sec: int | None = None,
) -> dict[str, Any]:
    command = [str(context.interpreter.python_executable), "-m", "pytest", *(arguments or [])]
    return run_command(context, command, timeout_sec=timeout_sec)


def run_ruff_check(
    context: ProjectMcpContext,
    *,
    arguments: list[str] | None = None,
    timeout_sec: int = 300,
) -> dict[str, Any]:
    command = [str(context.interpreter.python_executable), "-m", "ruff", "check", *(arguments or [])]
    return run_command(context, command, timeout_sec=timeout_sec)


def run_gen_protos(context: ProjectMcpContext, *, timeout_sec: int = 300) -> dict[str, Any]:
    command = [str(context.interpreter.python_executable), "-m", "contracts.tools.gen_protos"]
    return run_command(
        context,
        command,
        timeout_sec=timeout_sec,
        extra_env={"PYTHONIOENCODING": "utf-8"},
    )


_VALID_DATETIME_OPERATORS = {">", "<", ">=", "<=", "=="}
_DATETIME_OPERATOR_ALIASES = {
    "=>": ">=",
    "=<": "<=",
}


_READ_ONLY_SQL_QUERY_TYPES = {"select", "show", "describe", "explain"}


def _extract_sql_query_type(query: str) -> str:
    match = re.match(r"^\s*([a-zA-Z]+)", query)
    if not match:
        return "unknown"
    return match.group(1).strip().lower()


def _serialize_db_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _serialize_db_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_db_value(item) for item in value]
    return str(value)


def _normalize_datetime_operator(operator: str) -> str:
    normalized = _DATETIME_OPERATOR_ALIASES.get(operator, operator)
    if normalized not in _VALID_DATETIME_OPERATORS:
        raise ValueError(
            f"Unsupported datetime operator: {operator}. "
            f"Allowed: {sorted(_VALID_DATETIME_OPERATORS | set(_DATETIME_OPERATOR_ALIASES))}"
        )
    return normalized


def _normalize_created_at_operator(operator: str) -> str:
    return _normalize_datetime_operator(operator)


def _parse_datetime_value(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"

    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_created_at_value(value: str) -> datetime:
    return _parse_datetime_value(value)


def _to_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _validate_limit_offset(limit: int, offset: int) -> dict[str, Any] | None:
    if limit < 1 or limit > 1000:
        return {"success": False, "error": "limit must be between 1 and 1000"}
    if offset < 0:
        return {"success": False, "error": "offset must be >= 0"}
    return None


def _run_async(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001
            error["value"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if "value" in error:
        raise error["value"]
    return result.get("value")


def _current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_changelog_entry(
    context: ProjectMcpContext,
    *,
    text: str,
    file_name: str = "AGENTS_CHANGELOGS.md",
) -> dict[str, Any]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return {
            "success": False,
            "path": str(context.project_root / file_name),
            "error": "Changelog text must not be empty",
        }

    changelog_path = (context.project_root / file_name).resolve()
    try:
        changelog_path.relative_to(context.project_root.resolve())
    except ValueError:
        return {
            "success": False,
            "path": str(changelog_path),
            "error": "Changelog path must be inside project root",
        }

    timestamp = _current_timestamp()
    entry_lines = [f"### {timestamp}", *[f"- {line}" for line in lines]]
    entry_text = "\n".join(entry_lines) + "\n"

    existing = changelog_path.read_text(encoding="utf-8") if changelog_path.exists() else ""
    if existing and not existing.endswith("\n"):
        existing += "\n"
    if existing and not existing.endswith("\n\n"):
        existing += "\n"

    changelog_path.write_text(existing + entry_text, encoding="utf-8")

    return {
        "success": True,
        "timestamp": timestamp,
        "path": str(changelog_path),
        "entry": entry_lines,
    }
