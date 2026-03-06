---
name: experiment-mapping-lab
description: Run iterative experiments for cross-source event matching accuracy in arb_monitor. Use when the user asks to plan, implement, execute, and document a new mapping experiment between sources (especially polymarket and bcgame), including updates to tmp/experiments/* README files and INSIGHTS.md.
---

# Experiment Mapping Lab

Execute the full experiment cycle end-to-end with precision-first defaults.

## Core workflow

1. Read `tmp/experiments/README.md`, `tmp/experiments/INSIGHTS.md`, and the latest 1-3 experiment package `README.md` files.
2. Recheck at least one previous experiment on new data:
- read sources and `parsed_at` listed in that experiment `README.md`;
- query current `MAX(parsed_at)` for the same sources in DB;
- if current `MAX(parsed_at)` is newer, rerun that previous experiment on the new snapshot.
3. If rerun shows divergences or precision drop:
- write root cause notes in `## Дополнительно` of that previous experiment `README.md`;
- add transferable findings to `tmp/experiments/INSIGHTS.md`.
4. Determine the next package name: `tmp/experiments/{experiment_number:04}_{experiment_name}`.
5. Create the new package and write initial `README.md` with:
- hypothesis
- difference from previous experiment
- method
- run command
- sources with exact `parsed_at`
- planned metrics
6. Implement experiment code inside that package (`experiment.py` by default).
7. Run the experiment on current data.
8. Save artifacts in the package (`results.json`, `matched_pairs.csv`, and optional diagnostics CSV).
9. Update the package `README.md` with actual numbers, findings, limitations, exact sources+`parsed_at`, and next-step proposal.
10. Add transferable learnings to `tmp/experiments/INSIGHTS.md`.

## Precision-first defaults

- Use latest snapshot per source from `market_items` (`MAX(parsed_at)` grouped by `source`).
- Restrict candidate pool by canonical sport and start-time window.
- Prefer one-to-one mapping (`mutual best`) before lowering thresholds.
- Report both coverage and risk indicators:
- accepted matches count and share per source
- threshold/margin values used
- ambiguity indicators (top1-top2 gap)
- manual sanity sample (at least 10-20 pairs when no ground truth)

## Repository-specific rules

- Keep each experiment isolated; do not edit old experiment code unless explicitly requested.
- Keep numbering monotonic (`0001`, `0002`, ...).
- Keep all experiment files under `tmp/experiments`.
- If `tmp/experiments/README.md` or `INSIGHTS.md` is missing, initialize both before starting a new experiment.

## Helper script

Use `scripts/create_experiment.py` in this skill to scaffold the next experiment package quickly.

Example:
```powershell
python .codex/skills/experiment-mapping-lab/scripts/create_experiment.py --repo-root C:\dev\projects\arb_monitor --experiment-name sport-time-title-v2
```

The script creates:
- package directory with next numeric prefix
- `__init__.py`
- starter `README.md`
