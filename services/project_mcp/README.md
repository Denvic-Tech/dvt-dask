# project_mcp

FastMCP server for project tooling.

## Features
- Loads project `.env` from repository root at startup.
- Resolves project Python interpreter with priority:
  1. `PROJECT_VENV_PATH` (path to venv dir or `python.exe`).
  2. Scan project root subdirectories for `<dir>/Scripts/python.exe` and pick first interpreter
     matching `pyproject.toml:[project].requires-python`.
  3. Fallback to `sys.executable`.
- Provides tools for Python execution (`inline`, `subprocess`, file run), session handling, pytest and DB queries.

## Tools
- `get_runtime_info`: inspect resolved interpreter, loaded env keys and runtime status.
- `run_python_code`: execute inline/subprocess Python snippets in project interpreter.
- `run_python_file`: run a Python file inside project root.
- `list_inline_sessions`, `clear_inline_session`: manage inline execution sessions.
- `run_pytest`: run `pytest` via resolved interpreter.
- `run_db_query`: execute SQL through `src.db.engine`.
  - By default only read-only query types are allowed: `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`.
  - For write operations (`INSERT`/`UPDATE`/`DELETE`/DDL) pass `allow_write=true`.
  - Supports named SQL parameters (`:param`) via `parameters` dictionary.
  - Returns rows in JSON-safe form (`datetime` as ISO strings, `Decimal` as strings, bytes as hex).
- `append_changelog_entry`: append timestamped records to `AGENTS_CHANGELOGS.md`.

## Run
```bash
{venv_dir_path}/Scripts/python.exe -m scripts.run_project_mcp
```

Optional:
```bash
{venv_dir_path}/Scripts/python.exe -m scripts.run_project_mcp --no-reexec
```
