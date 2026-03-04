# Repository Guidelines

## Project Structure & Module Organization
Core library code lives in `dask/` with major subpackages like `array/`, `dataframe/`, `bag/`, `bytes/`, `diagnostics/`, and `widgets/`.  
Tests are colocated with modules under `dask/**/tests/` (for example, `dask/array/tests/test_*.py`) with additional integration-style coverage in `dask/tests/`.  
Documentation sources are in `docs/source/` (reStructuredText), while CI and environment definitions are in `.github/workflows/` and `continuous_integration/`.

## Build, Test, and Development Commands
- `python -m pip install -e ".[complete,test]"` installs Dask in editable mode with common optional and test dependencies.
- `python -m pytest dask -v` runs the main test suite.
- `python -m pytest dask/dataframe/tests/test_dataframe.py::test_rename_index` runs a single test.
- `python -m pytest dask --runslow -n auto` includes slow tests and parallel execution (`pytest-xdist`).
- `pre-commit run --all-files` runs linting/formatting/type checks used by CI.
- `cd docs && python -m pip install -r requirements-docs.txt && make html` builds local docs.

## Coding Style & Naming Conventions
Use Python 3.10+ features compatible with project constraints (`requires-python >=3.10`).  
Formatting and linting are enforced through `pre-commit` with Ruff and Black; line length is 120.  
Follow standard Python naming: `snake_case` for functions/modules, `PascalCase` for classes, `UPPER_CASE` for constants. Keep public API/docstrings clear and numpydoc-friendly.

## Testing Guidelines
Framework: `pytest` with strict markers/config (`--strict-config`, `--strict-markers`).  
Use existing markers (`slow`, `network`, `gpu`, `array_expr`) explicitly when relevant.  
For bug fixes, add a focused regression test near the affected module.  
Coverage is tracked in CI/codecov with an 88% project target; avoid reducing effective coverage in touched areas.

## Commit & Pull Request Guidelines
Recent history favors concise, imperative subjects, sometimes with prefixes like `fix:` or `Doc:`, and often ending with PR refs (example: `Fix flaky categorical concat test (#12276)`).  
PRs should satisfy the repository template:
- link/close an issue (`Closes #xxxx`)
- include added or updated tests
- pass `pre-commit run --all-files`

## Configuration Tips
When adding config options, keep `dask/dask.yaml` and `dask/dask-schema.yaml` in sync.  
Do not commit local environment artifacts (`.venv/`, machine-specific IDE files, generated caches).
