# Agent Operating Contract

This repository is an AI-agent-driven quantitative research platform.

## Non-Negotiable Rules

1. `finmind-dl` is the official ingestion tool.
2. Agents must not bypass local persistence by calling remote APIs inside research modules.
3. Data ingestion and research logic must remain separate.
4. Agents must not overwrite prior experiment outputs.
5. All experiments must be reproducible and registry-tracked.
6. Assumptions and key decisions must be documented.
7. Prefer additive refactoring over destructive changes.
8. Preserve compatibility with existing strategy and downloader behavior.
9. Every major experiment run should leave structured artifacts in `experiments/<id>/`.
10. Agents should append notable decisions to `memory-bank/decision-log/`.

## Data Contract

Canonical flow:

`FinMind API -> finmind-dl -> SQLite/local files -> research loaders -> universe -> features -> strategy -> backtest -> statistics -> registry -> report`

## Safety

- Do not delete existing datasets or experiment folders unless explicitly instructed.
- Avoid in-place edits to historical experiment outputs.
- Validate local data availability before running strategy research.
