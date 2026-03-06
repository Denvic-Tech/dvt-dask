from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"


@dataclass(frozen=True)
class InterpreterResolution:
    python_executable: Path
    python_version: str
    source: str
    requires_python: str | None
    is_version_match: bool
    scanned_candidates: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "python_executable": str(self.python_executable),
            "python_version": self.python_version,
            "source": self.source,
            "requires_python": self.requires_python,
            "is_version_match": self.is_version_match,
            "scanned_candidates": list(self.scanned_candidates),
        }


def load_project_env(env_path: Path = DOTENV_PATH, *, override: bool = False) -> dict[str, str]:
    """Load .env variables into process environment."""
    loaded: dict[str, str] = {}
    if not env_path.exists():
        return loaded

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.lower().startswith("export "):
            line = line[7:].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value
            loaded[key] = value

    return loaded


def read_requires_python(pyproject_path: Path = PYPROJECT_PATH) -> str | None:
    """Read [project].requires-python from pyproject.toml."""
    if not pyproject_path.exists():
        return None

    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project_data = data.get("project", {})
    requires_python = project_data.get("requires-python")

    if isinstance(requires_python, str):
        stripped = requires_python.strip()
        return stripped or None

    return None


def _build_specifier(requires_python: str) -> SpecifierSet | None:
    try:
        return SpecifierSet(requires_python)
    except InvalidSpecifier:
        try:
            return SpecifierSet(f"=={requires_python}.*")
        except InvalidSpecifier:
            return None


def version_matches_requires(version: str, requires_python: str | None) -> bool:
    """Check whether a Python version matches requires-python constraint."""
    if not requires_python:
        return True

    specifier = _build_specifier(requires_python)
    if specifier is None:
        return version.startswith(requires_python)

    try:
        parsed = Version(version)
    except InvalidVersion:
        return False

    return parsed in specifier


def probe_python_version(python_executable: Path, *, timeout_sec: int = 8) -> str | None:
    """Return Python version for the executable, or None on probe failure."""
    try:
        completed = subprocess.run(
            [
                str(python_executable),
                "-c",
                "import sys; print('.'.join(str(v) for v in sys.version_info[:3]))",
            ],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    version = completed.stdout.strip()
    return version or None


def _resolve_from_env_var(
    venv_hint: str,
    requires_python: str | None,
) -> InterpreterResolution:
    raw_path = Path(venv_hint).expanduser()
    python_path = raw_path / "Scripts" / "python.exe" if raw_path.is_dir() else raw_path

    if not python_path.exists():
        raise FileNotFoundError(f"PROJECT_VENV_PATH points to missing path: {python_path}")

    version = probe_python_version(python_path)
    if version is None:
        raise RuntimeError(f"Cannot probe Python version from PROJECT_VENV_PATH: {python_path}")

    return InterpreterResolution(
        python_executable=python_path,
        python_version=version,
        source="PROJECT_VENV_PATH",
        requires_python=requires_python,
        is_version_match=version_matches_requires(version, requires_python),
        scanned_candidates=(),
    )


def resolve_project_interpreter(
    project_root: Path = PROJECT_ROOT,
    pyproject_path: Path = PYPROJECT_PATH,
) -> InterpreterResolution:
    """Resolve project Python interpreter with PROJECT_VENV_PATH priority."""
    requires_python = read_requires_python(pyproject_path)

    venv_hint = os.getenv("PROJECT_VENV_PATH")
    if venv_hint:
        return _resolve_from_env_var(venv_hint, requires_python)

    scanned: list[str] = []
    for child in sorted(project_root.iterdir(), key=lambda item: item.name.lower()):
        if not child.is_dir():
            continue

        candidate = child / "Scripts" / "python.exe"
        if not candidate.is_file():
            continue

        version = probe_python_version(candidate)
        if version is None:
            continue

        scanned.append(str(candidate))
        if version_matches_requires(version, requires_python):
            return InterpreterResolution(
                python_executable=candidate,
                python_version=version,
                source=f"scan:{child.name}",
                requires_python=requires_python,
                is_version_match=True,
                scanned_candidates=tuple(scanned),
            )

    fallback_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    return InterpreterResolution(
        python_executable=Path(sys.executable),
        python_version=fallback_version,
        source="sys.executable",
        requires_python=requires_python,
        is_version_match=version_matches_requires(fallback_version, requires_python),
        scanned_candidates=tuple(scanned),
    )


def same_python_executable(left: Path, right: Path) -> bool:
    """Cross-platform executable path equality check."""
    try:
        return left.resolve().samefile(right.resolve())
    except OSError:
        return str(left.resolve()).lower() == str(right.resolve()).lower()
