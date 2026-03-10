# Research Specs

`research_specs/` stores stable study definitions.

A research spec defines the study once:

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

The spec is intentionally separate from any one execution.

A run is created by resolving the spec with a `data_as_of` cutoff:

```bash
python -m research.run --spec research_specs/ma_cross_example_v1.json --data-as-of 2026-03-31
```

This produces an append-only run directory under:

`experiments/<research_id>/runs/<run_id>/`

with:

- `resolved_spec.json`
- `data_manifest.json`
- `metrics.json`
- `report.md`
- `artifacts.json`
- `run.log`
- `plots/`

Current supported rerun mode:

- `fixed_spec`

The recommended pattern is:

1. define the study once in `research_specs/<research_id>.json`
2. update local data through `finmind-dl`
3. rerun the spec with a newer `--data-as-of`
4. compare runs with `python -m research.compare_runs`
