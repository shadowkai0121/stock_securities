# Research Pipeline

## Standard Pipeline

1. Resolve configuration.
2. Resolve `data_as_of` for the run.
3. Validate dataset availability.
4. Ingest missing or stale local data through `finmind-dl` wrappers.
5. Construct tradable universe using only local data up to `data_as_of`.
6. Generate or load cached features.
7. Run strategy signal model.
8. Run shared backtest engine.
9. Run statistical validation.
10. Generate report artifacts.
11. Register append-only run metadata and outputs.

## Rerun Entry Point

Preferred rerun command:

```bash
python -m research.run --spec research_specs/ma_cross_example_v1.json --data-as-of 2026-03-31
```

The runner writes:

- `resolved_spec.json`
- `data_manifest.json`
- `metrics.json`
- `report.md`
- `artifacts.json`
- `run.log`

## Replaceable Interfaces

`research/pipeline.py` defines standardized components:

- DataLoader
- UniverseBuilder
- FeaturePipeline
- SignalModel
- PortfolioConstructor
- CostModel
- BacktestEngine
- Evaluator
- ReportGenerator

These interfaces allow new research modules to plug in without rewriting the full workflow.
