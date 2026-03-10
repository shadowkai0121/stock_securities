# Research Pipeline

## Full Empirical Lifecycle

1. Define a stable research spec (`research_specs/<research_id>.json`).
2. Resolve `data_as_of` and validate local dataset coverage.
3. Ingest missing/stale data via `finmind-dl` only.
4. Build universe and feature panel from local persistence.
5. Run strategy and backtest.
6. Compute statistical validation and inference panel artifacts.
7. Optionally run robustness scenario grid.
8. Register append-only run outputs under `experiments/<research_id>/runs/<run_id>/`.
9. Generate paper artifacts under `papers/<paper_id>/`.
10. Persist reproducibility records (spec, run IDs, data manifest, environment info).

## Rerun Entry Point

```bash
python -m research.run --spec research_specs/ma_cross_example_v1.json --data-as-of 2026-03-31
```

Core run outputs:

- `resolved_spec.json`
- `data_manifest.json`
- `metrics.json`
- `artifacts.json`
- `backtest_timeseries.csv`
- `inference_panel.csv` (when available)
- `report.md`
- `run.log`
- optional `robustness/robustness_results.json`

## Paper Artifact Entry Point

```bash
python -m research.paper_outputs.generate --experiment <run_id> --paper <paper_id>
```

Paper workspace outputs:

- `papers/<paper_id>/tables/` (CSV/Markdown/LaTeX)
- `papers/<paper_id>/figures/` (PNG)
- `papers/<paper_id>/appendix/` (appendix tables)
- `papers/<paper_id>/reproducibility/` (`research_spec.json`, `experiment_run_ids.txt`, `data_manifest.json`, `environment_info.json`)

## Inference Comparison

```bash
python -m research.compare_inference --research-id <research_id> --base-run <run_a> --target-run <run_b>
```

This compares:

- coefficient stability
- t-stat stability
- significance persistence
- portfolio spread changes
