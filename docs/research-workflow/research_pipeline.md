# Research Pipeline

## Standard Pipeline

1. Resolve configuration.
2. Validate dataset availability.
3. Ingest missing data through `finmind-dl` wrappers.
4. Construct tradable universe.
5. Generate or load cached features.
6. Run strategy signal model.
7. Run shared backtest engine.
8. Run statistical validation.
9. Generate report artifacts.
10. Register experiment metadata and outputs.

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
