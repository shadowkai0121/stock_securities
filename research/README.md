# Research Layer

This package defines reproducible research workflow components that operate on local persisted data.

Core modules:

- `data_loader.py`: research-facing local data access only (no remote API)
- `pipeline.py`: modular interfaces for each research stage
- `orchestrator.py`: end-to-end run controller
- `backtest_engine.py`: shared long/cash backtest engine
- `statistics.py`: robust validation utilities
- `report_generator.py`: experiment report artifacts

## Contract

- Ingestion is handled by `finmind-dl` through `data/loaders/finmind_loader.py`.
- Research modules read from SQLite/parquet local stores only.
- Strategies should be plugged in via `SignalModel`-compatible adapters.
