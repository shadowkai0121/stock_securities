# Agent Roles

## Data Agent

- Owns ingestion planning and execution via `finmind-dl` wrappers.
- Ensures required local datasets exist before research starts.
- Runs dataset quality checks and reports gaps.

## Research Agent

- Proposes and versions research specs under `research_specs/`.
- Generates/extends local feature definitions compatible with the existing feature store.
- Runs experiments through append-only run workflows.
- Produces inference outputs, paper tables/figures, and reproducibility artifacts.

## Backtest Agent

- Uses shared backtest engine interfaces only.
- Enforces lagged execution, transaction costs, and turnover accounting.
- Outputs consistent metrics for cross-strategy comparability.

## Reviewer Agent

- Detects look-ahead bias risks in feature, signal, and execution logic.
- Detects survivorship bias risks in universe definition and data coverage assumptions.
- Detects data leakage across train/validation/test and event windows.
- Reviews statistical validity and reproducibility integrity.

## Replication Agent

- Re-runs experiments from saved specs/run IDs without mutating historical outputs.
- Validates that inference, tables, and figures can be regenerated.
- Compares replicated outputs against prior runs and documents drift causes.
