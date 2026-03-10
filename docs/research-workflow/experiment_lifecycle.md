# Experiment Lifecycle

## Create

- allocate unique `experiment_id`
- create folder and fixed artifact structure
- snapshot config and metadata

## Rerun Create

- allocate unique `run_id`
- create `experiments/<research_id>/runs/<run_id>/`
- snapshot `resolved_spec.json`
- snapshot `data_manifest.json`
- seed `run.log`, `metrics.json`, `artifacts.json`, `report.md`

## Execute

- run orchestrator pipeline end-to-end
- capture metrics and validation outputs
- generate report and plots
- generate inference panel and event candidates
- execute optional robustness scenario grid

## Finalize

- write `metrics.json`
- write `artifacts.json`
- keep `report.md` as human-readable summary
- store `inference_results.json` when inference is computed

## Paper Outputs

- run `python -m research.paper_outputs.generate --experiment <run_id> --paper <paper_id>`
- export tables in CSV/Markdown/LaTeX
- export figures for cumulative returns, coefficients, spreads, and rolling performance
- write reproducibility payloads under `papers/<paper_id>/reproducibility/`

## Reproduce

- rerun with saved `config.json`
- compare outputs to previous metrics/artifacts
- log divergence analysis in memory bank

## Non-Overwrite Policy

Historical experiment folders are immutable records and must not be overwritten.

The same rule now applies to research reruns:

- each `run_id` is append-only
- `latest.json` and `run_index.json` are convenience pointers only
- previous run folders remain untouched
