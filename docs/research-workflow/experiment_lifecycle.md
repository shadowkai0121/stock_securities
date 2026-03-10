# Experiment Lifecycle

## Create

- allocate unique `experiment_id`
- create folder and fixed artifact structure
- snapshot config and metadata

## Execute

- run orchestrator pipeline end-to-end
- capture metrics and validation outputs
- generate report and plots

## Finalize

- write `metrics.json`
- write `artifacts.json`
- keep `report.md` as human-readable summary

## Reproduce

- rerun with saved `config.json`
- compare outputs to previous metrics/artifacts
- log divergence analysis in memory bank

## Non-Overwrite Policy

Historical experiment folders are immutable records and must not be overwritten.
