#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

EXPERIMENT_DIR_PATTERN = re.compile(r"^(\d{4})_(.+)$")

README_TEMPLATE = """# {title}

## Hypothesis
Describe what should improve and why.

## Difference From Previous Experiment
- Describe concrete changes vs previous experiment.

## Method
- Data scope:
- Candidate generation:
- Scoring or rules:
- Acceptance criteria:

## Sources And Parsed At
- source: `parsed_at`

## Run Command
```powershell
python tmp/experiments/{package}/experiment.py
```

## Artifacts
- `results.json`
- `matched_pairs.csv`
- `top2_candidates.csv` (optional)

## Results
Fill after execution.

## Additional
- Previous experiment recheck on newer data:
- Divergences/precision drop analysis (if any):
"""


def find_next_number(experiments_dir: Path) -> int:
    max_num = 0
    for child in experiments_dir.iterdir():
        if not child.is_dir():
            continue
        match = EXPERIMENT_DIR_PATTERN.match(child.name)
        if not match:
            continue
        max_num = max(max_num, int(match.group(1)))
    return max_num + 1


def normalize_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s_]", "", name).strip().lower()
    cleaned = re.sub(r"[\s\-]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    if not cleaned:
        raise ValueError("Experiment name is empty after normalization")
    return cleaned


def create_package(repo_root: Path, experiment_name: str, dry_run: bool) -> Path:
    experiments_dir = repo_root / "tmp" / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)

    number = find_next_number(experiments_dir)
    slug = normalize_name(experiment_name)
    package_name = f"{number:04}_{slug}"
    package_dir = experiments_dir / package_name

    if dry_run:
        return package_dir

    package_dir.mkdir(parents=False, exist_ok=False)

    init_text = f'"""Experiment package: {package_name}."""\n'
    (package_dir / "__init__.py").write_text(init_text, encoding="utf-8")

    readme_title = f"{number:04} {slug.replace('_', ' ').title()}"
    readme_text = README_TEMPLATE.format(title=readme_title, package=package_name)
    (package_dir / "README.md").write_text(readme_text, encoding="utf-8")

    return package_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the next experiment package in tmp/experiments")
    parser.add_argument("--repo-root", required=True, help="Repository root path")
    parser.add_argument("--experiment-name", required=True, help="Experiment name slug or phrase")
    parser.add_argument("--dry-run", action="store_true", help="Only print target directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    package_dir = create_package(
        repo_root=repo_root,
        experiment_name=args.experiment_name,
        dry_run=args.dry_run,
    )
    print(str(package_dir))


if __name__ == "__main__":
    main()
