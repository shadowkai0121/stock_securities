# Experiments

This directory contains experiment outputs and registry utilities.

Legacy orchestrated experiments are stored under:

`experiments/<experiment_id>/`

with:

- `config.json`
- `metrics.json`
- `report.md`
- `artifacts.json`
- `plots/`

Use `registry.py` to ensure reproducible metadata capture.

Append-only research reruns are stored under:

`experiments/<research_id>/runs/<run_id>/`

with:

- `run_metadata.json`
- `resolved_spec.json`
- `data_manifest.json`
- `metrics.json`
- `report.md`
- `artifacts.json`
- `run.log`
- `plots/`

Convenience files:

- `experiments/<research_id>/latest.json`
- `experiments/<research_id>/run_index.json`
