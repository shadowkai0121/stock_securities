# Agent Roles

## Data Agent

- Owns ingestion planning and execution via `finmind-dl` wrappers.
- Ensures required local datasets exist before research starts.
- Runs dataset quality checks and reports gaps.

## Research Agent

- Builds universe definitions and feature pipelines from local data.
- Designs and runs strategy experiments with explicit assumptions.
- Produces reproducible configs and parameter tracking.

## Backtest Agent

- Uses shared backtest engine interfaces only.
- Enforces lagged execution, transaction costs, and turnover accounting.
- Outputs consistent metrics for cross-strategy comparability.

## Reviewer Agent

- Reviews methodology, statistical validity, and implementation risks.
- Checks reproducibility and experiment metadata integrity.
- Flags potential look-ahead bias, leakage, and survivorship issues.

## Reproduction Agent

- Re-runs historical experiments from saved configs/artifacts.
- Confirms metrics and outputs are reproducible.
- Documents drift causes when outputs diverge.
