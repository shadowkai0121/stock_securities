# Research Spec + Run Workflow

## What A Research Spec Is

A research spec is a stable JSON file under `research_specs/` that defines the study independently from any single execution.

Typical fields:

- `research_id`
- `title`
- `description`
- `pipeline_id`
- `required_datasets`
- `data_update_policy`
- `analysis_period`
- `universe_definition`
- `feature_definition`
- `strategy_definition`
- `backtest_definition`
- `evaluation_definition`
- `rerun_mode`
- `report_definition`
- optional `robustness`
- optional `companion_docs`

Supported rerun mode today:

- `fixed_spec`

## What A Run Is

A run is one append-only execution of a research spec against a particular `data_as_of` cutoff.

Run layout:

`experiments/<research_id>/runs/<run_id>/`

Files:

- `run_metadata.json`
- `resolved_spec.json`
- `data_manifest.json`
- `metrics.json`
- `report.md`
- `artifacts.json`
- `run.log`
- `plots/`
- optional `robustness/`

Convenience files:

- `experiments/<research_id>/latest.json`
- `experiments/<research_id>/run_index.json`

## How `finmind-dl` Fits Into Reruns

`finmind-dl` remains the official ingestion boundary.

The rerun runner:

1. reads the research spec
2. resolves `data_as_of`
3. checks local SQLite coverage for required datasets
4. invokes the Python wrapper around `finmind-dl` only when local data is missing or stale
5. validates local tables
6. runs the study using local persisted data only

Research modules do not call remote APIs directly.

## How To Define A New Research

1. Copy `research_specs/ma_cross_example_v1.json`.
2. Change `research_id`, title, description, and the study definitions.
3. Keep `required_datasets` aligned with the local datasets the study needs.
4. Set `analysis_period.start_date` to the first in-sample date.
5. Use `pipeline_id` to select the study executor.
6. Keep the spec stable; reruns should change `data_as_of`, not the base spec.
7. Add `research_specs/<research_id>.hypothesis.md` and `research_specs/<research_id>.design.md` to document identification assumptions.
8. Add optional `robustness` grids for transaction costs, holding periods, and winsorization levels.

## How To Rerun An Existing Research On Newer Data

Manual command:

```bash
python -m research.run --spec research_specs/ma_cross_example_v1.json --data-as-of 2026-03-31
```

Useful overrides:

```bash
python -m research.run ^
  --spec research_specs/ma_cross_example_v1.json ^
  --data-as-of 2026-03-31 ^
  --data-root data ^
  --experiments-root experiments ^
  --feature-store-version v1
```

Result:

- new run directory under `experiments/<research_id>/runs/<run_id>/`
- prior runs untouched
- `latest.json` updated to the newest successful run

## How To Compare Runs

Compare the latest run against the previous successful run:

```bash
python -m research.compare_runs --research-id ma_cross_example_v1
```

Compare two explicit runs and also save JSON:

```bash
python -m research.compare_runs ^
  --research-id ma_cross_example_v1 ^
  --base-run 20260310T010203000000Z__asof_20260301 ^
  --target-run 20260401T010203000000Z__asof_20260331 ^
  --output-json compare_runs.json
```

Current comparison output includes:

- `annual_return`
- `annual_volatility`
- `sharpe_ratio`
- `max_drawdown`
- `turnover`
- `number_of_trades`
- `universe_size`
- dataset row-count and max-date changes from `data_manifest.json`

Inference comparison:

```bash
python -m research.compare_inference --research-id ma_cross_example_v1
```

This adds:

- coefficient stability
- t-stat stability
- significance persistence
- long-short spread changes

## How To Schedule Periodic Reruns

The runner is automation-friendly:

- non-interactive CLI
- explicit `--spec` and `--data-as-of`
- deterministic directory structure
- append-only run folders
- `run.log` inside each run
- exit code `0` on success
- exit code `2` for spec/path validation errors
- exit code `1` for runtime/study failures

Examples:

- cron: run `python -m research.run ...` on a daily or weekly schedule
- Windows Task Scheduler: set the repo root as the working directory and call the same command
- CI / GitHub Actions: run the same CLI after a `finmind-dl`-backed data refresh step or let the runner refresh local data if allowed

## Data-As-Of Semantics

`data_as_of` means the latest date the run is allowed to use.

The runner enforces this by:

- limiting local loader queries to `end_date=data_as_of`
- storing `data_as_of` in `resolved_spec.json`
- recording `data_as_of` in `data_manifest.json`
- writing run-level metrics and reports from the cutoff-bounded data only

## Generate Paper Artifacts

After a run completes:

```bash
python -m research.paper_outputs.generate --experiment <run_id> --paper <paper_id>
```

This builds:

1. inference results (`inference_results.json`)
2. paper-ready tables (`papers/<paper_id>/tables/`)
3. paper-ready figures (`papers/<paper_id>/figures/`)
4. appendix tables (`papers/<paper_id>/appendix/`)
5. reproducibility records (`papers/<paper_id>/reproducibility/`)
