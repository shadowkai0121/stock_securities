# Data Flow

Canonical data movement is intentionally one-directional:

`FinMind API -> finmind-dl -> SQLite -> research loader -> universe -> feature store -> strategy -> backtest -> inference/statistics -> run registry -> paper outputs`

## Principles

- Research code reads local data only.
- Ingestion and research responsibilities are separated.
- Missing datasets are handled by orchestrator-triggered ingestion wrappers.

## Operational Sequence

1. Orchestrator inspects required datasets.
2. If missing, orchestrator calls `FinMindLoader` methods.
3. `FinMindLoader` delegates to existing `finmind_dl` handlers.
4. Handlers write normalized tables to SQLite.
5. `ResearchDataLoader` reads local tables into analysis frames.
6. Universe and features are built from those local frames.
7. Strategy and backtest run without any remote data access.
8. Rerun outputs are registered under `experiments/<research_id>/runs/<run_id>/`.
9. `latest.json` and `run_index.json` provide lightweight filesystem pointers for comparison tooling.
10. Inference outputs can be compared across runs (`research.compare_inference`).
11. Paper tables/figures/reproducibility artifacts are generated under `papers/<paper_id>/`.
