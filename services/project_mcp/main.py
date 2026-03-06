from __future__ import annotations

import argparse
import importlib
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

from fastmcp import FastMCP

from services.project_mcp.operations import ProjectMcpContext
from services.project_mcp.runtime import (
    PROJECT_ROOT,
    InterpreterResolution,
    load_project_env,
    resolve_project_interpreter,
    same_python_executable,
)


ENV_LOADED_KEYS = load_project_env()
INTERPRETER: InterpreterResolution = resolve_project_interpreter()
CONTEXT = ProjectMcpContext(
    project_root=PROJECT_ROOT,
    env_loaded_keys=ENV_LOADED_KEYS,
    interpreter=INTERPRETER,
)

mcp = FastMCP(name="project_mcp")

HOT_RELOAD_ENABLED = os.getenv("PROJECT_MCP_HOT_RELOAD", "true").lower() in {"1", "true", "yes"}
HOT_RELOAD_LAST_ERROR: str | None = None


def _module_mtime_ns(module: ModuleType) -> int:
    module_file = getattr(module, "__file__", None)
    if not module_file:
        return 0

    try:
        return Path(module_file).resolve().stat().st_mtime_ns
    except OSError:
        return 0


_OPERATIONS_MODULE: ModuleType = importlib.import_module("services.project_mcp.operations")
_OPERATIONS_MODULE_MTIME_NS = _module_mtime_ns(_OPERATIONS_MODULE)


def _get_operations_module() -> ModuleType:
    global _OPERATIONS_MODULE
    global _OPERATIONS_MODULE_MTIME_NS
    global HOT_RELOAD_LAST_ERROR

    if not HOT_RELOAD_ENABLED:
        return _OPERATIONS_MODULE

    current_mtime = _module_mtime_ns(_OPERATIONS_MODULE)
    if current_mtime <= _OPERATIONS_MODULE_MTIME_NS:
        return _OPERATIONS_MODULE

    try:
        _OPERATIONS_MODULE = importlib.reload(_OPERATIONS_MODULE)
        _OPERATIONS_MODULE_MTIME_NS = _module_mtime_ns(_OPERATIONS_MODULE)
        HOT_RELOAD_LAST_ERROR = None
    except Exception as exc:  # noqa: BLE001
        HOT_RELOAD_LAST_ERROR = f"{type(exc).__name__}: {exc}"

    return _OPERATIONS_MODULE


def _ensure_server_python() -> None:
    marker = "PROJECT_MCP_REEXEC_DONE"
    if os.getenv(marker) == "1":
        return

    if same_python_executable(Path(sys.executable), INTERPRETER.python_executable):
        return

    env = dict(os.environ)
    env[marker] = "1"
    command = [str(INTERPRETER.python_executable), "-m", "scripts.run_project_mcp", *sys.argv[1:]]
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT), env=env, check=False)
    raise SystemExit(completed.returncode)


@mcp.tool(description="Show current project runtime and interpreter resolution details")
def get_runtime_info(
    services: list[str] | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    info = _get_operations_module().get_runtime_info(CONTEXT, services=services, verbose=verbose)
    info["hot_reload"] = {
        "enabled": HOT_RELOAD_ENABLED,
        "module": "services.project_mcp.operations",
        "last_error": HOT_RELOAD_LAST_ERROR,
    }
    return info


@mcp.tool(description="Run Python code using project interpreter (inline or subprocess mode)")
def run_python_code(
    code: str,
    execution_mode: Literal["inline", "subprocess"] = "inline",
    session_id: str = "default",
    timeout_sec: int = 120,
) -> dict[str, Any]:
    return _get_operations_module().run_python_code(
        CONTEXT,
        code,
        execution_mode=execution_mode,
        session_id=session_id,
        timeout_sec=timeout_sec,
    )


@mcp.tool(description="Run a Python file with project interpreter")
def run_python_file(
    file_path: str,
    arguments: list[str] | None = None,
    timeout_sec: int = 300,
) -> dict[str, Any]:
    return _get_operations_module().run_python_file(
        CONTEXT,
        file_path,
        arguments=arguments,
        timeout_sec=timeout_sec,
    )


@mcp.tool(description="List active inline Python sessions")
def list_inline_sessions() -> list[str]:
    return _get_operations_module().list_inline_sessions(CONTEXT)


@mcp.tool(description="Clear one inline Python session or all sessions")
def clear_inline_session(session_id: str | None = None) -> dict[str, Any]:
    return _get_operations_module().clear_inline_session(CONTEXT, session_id=session_id)


@mcp.tool(description="Run project pytest via resolved interpreter")
def run_pytest(arguments: list[str] | None = None, timeout_sec: int | None = None) -> dict[str, Any]:
    return _get_operations_module().run_pytest(CONTEXT, arguments=arguments, timeout_sec=timeout_sec)


@mcp.tool(description="Append changelog entry with current timestamp to AGENTS_CHANGELOGS.md")
def append_changelog_entry(text: str) -> dict[str, Any]:
    return _get_operations_module().append_changelog_entry(CONTEXT, text=text)


def run_project_mcp(
    transport: Literal["stdio", "sse", "streamable-http"] = "stdio",
    *,
    ensure_project_interpreter: bool = True,
) -> None:
    if ensure_project_interpreter:
        _ensure_server_python()
    mcp.run(transport=transport)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run project FastMCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default=os.getenv("PROJECT_MCP_TRANSPORT", "stdio"),
    )
    parser.add_argument(
        "--no-reexec",
        action="store_true",
        help="Do not restart process with resolved project interpreter",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_project_mcp(transport=args.transport, ensure_project_interpreter=not args.no_reexec)
