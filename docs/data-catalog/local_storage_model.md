# Local Storage Model

## SQLite Tables

`finmind-dl` writes normalized tables (see `src/finmind_dl/schema.py`) such as:

- `price_daily`
- `price_adj_daily`
- `margin_daily`
- `broker_trades`
- `holding_shares_per`
- `stock_info`
- `warrant_summary`
- `meta_runs`

## Storage Pattern

- per-stock DBs: `data/<stock_id>.sqlite`
- shared market DB: `data/market.sqlite`
- feature cache: `data/feature_cache/<version>/<key>.parquet`
- research specs: `research_specs/<research_id>.json`
- append-only run outputs: `experiments/<research_id>/runs/<run_id>/`

## Why Research Reads Local Data Only

- deterministic reruns
- avoids API drift during analysis
- separates acquisition reliability from model logic
- supports offline / CI execution

## Run Audit Trail

Each rerun snapshots the local data state in `data_manifest.json`.

The manifest records:

- SQLite paths used
- tables used
- row counts up to `data_as_of`
- min/max date coverage
- file fingerprints
- latest `meta_runs` timestamps when available
