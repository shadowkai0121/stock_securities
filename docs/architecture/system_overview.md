# System Overview

## Purpose

This repository now acts as a reproducible quantitative research operating system with explicit AI-agent workflow support.

## Layered Architecture

1. Ingestion Layer: `finmind-dl` (`src/finmind_dl`) wrapped by `data/loaders/finmind_loader.py`
2. Storage Layer: local SQLite files under `data/` and strategy-specific DB paths
3. Data Access Layer: `research/data_loader.py`
4. Universe Layer: `universe/universe_builder.py`
5. Feature Layer: `features/feature_defs.py`, `features/feature_store.py`
6. Strategy Layer: adapters in `research/strategies/`
7. Backtest Layer: `research/backtest_engine.py`
8. Statistical Validation Layer: `research/statistics.py`
9. Experiment Tracking Layer: `experiments/registry.py`
10. Agent Operating Layer: `agents/`
11. Reporting Layer: `research/report_generator.py`

## Why `finmind-dl` Exists

`finmind-dl` centralizes:

- token handling
- API response normalization
- schema evolution
- idempotent upsert logic
- historical run logging (`meta_runs`)

Keeping this boundary stable prevents strategy modules from embedding fragile API logic.

## Reproducibility Guarantees

- local persisted datasets as source of truth
- cached feature panels with versioned keys
- experiment folders with config/metrics/artifacts/report
- optional git commit hash capture in registry metadata
