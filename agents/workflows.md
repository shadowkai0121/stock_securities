# Agent Workflows

## New Strategy Experiment

1. Load config and required datasets.
2. Validate local dataset coverage.
3. Run ingestion through `finmind-dl` if data is missing.
4. Build universe and feature panel.
5. Generate signals and run shared backtest engine.
6. Run statistical validation.
7. Register experiment and write report artifacts.

## Paper Reproduction

1. Convert paper assumptions into versioned config.
2. Freeze universe, features, and parameter definitions.
3. Ensure historical dataset snapshot availability.
4. Execute orchestrated pipeline.
5. Generate inference and paper artifacts with `python -m research.paper_outputs.generate`.
6. Compare output tables/figures with paper targets.
7. Document deviations in `memory-bank/paper-notes/`.

## Dataset Extension

1. Add dataset metadata to `data/catalog/data_catalog.yaml`.
2. Implement ingestion wrapper method in `data/loaders/finmind_loader.py`.
3. Add data-access support in `research/data_loader.py`.
4. Add quality checks and tests.
5. Update architecture docs.

## Failed Experiment Recovery

1. Inspect `metrics.json`, `artifacts.json`, and logs.
2. Identify failure class: data, model, backtest, or system.
3. Re-run from same config after fixing root cause.
4. Record lessons in `memory-bank/experiment-lessons/`.
5. Do not overwrite the original failed experiment folder.

## Inference Stability Review

1. Select two successful runs of the same `research_id`.
2. Run `python -m research.compare_inference`.
3. Inspect coefficient and t-stat stability.
4. Check significance persistence and long-short spread drift.
5. Log interpretation decisions in `memory-bank/decision-log/`.
