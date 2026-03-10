# Research Layer

This package defines reproducible research workflow components that operate on local persisted data.

Core modules:

- `data_loader.py`: research-facing local data access only (no remote API)
- `specs.py`: stable research specs and resolved run snapshots
- `data_state.py`: local dataset coverage, validation, and manifests
- `pipeline.py`: modular interfaces for each research stage
- `orchestrator.py`: end-to-end run controller
- `run.py`: append-only research spec runner
- `compare_runs.py`: run comparison CLI
- `compare_inference.py`: inference stability comparison CLI
- `backtest_engine.py`: shared long/cash backtest engine
- `statistics.py`: robust validation utilities
- `report_generator.py`: experiment report artifacts
- `inference/`: empirical inference methods (Fama-MacBeth, panel OLS, portfolio sorts, event studies)
- `paper_outputs/`: paper-ready tables/figures and reproducibility artifact generators

## Contract

- Ingestion is handled by `finmind-dl` through `data/loaders/finmind_loader.py`.
- Research modules read from SQLite/parquet local stores only.
- Strategies should be plugged in via `SignalModel`-compatible adapters.
- `data_as_of` is explicit in rerun orchestration and must bound all local reads.

## Paper Outputs

Generate paper artifacts from a run:

```bash
python -m research.paper_outputs.generate --experiment <run_id> --paper <paper_id>
```

Compare inference stability across runs:

```bash
python -m research.compare_inference --research-id <research_id> --base-run <run_a> --target-run <run_b>
```
