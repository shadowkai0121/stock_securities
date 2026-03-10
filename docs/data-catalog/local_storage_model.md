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
- shared metadata DBs: `data/stock_info.sqlite`, `data/holding_shares_per.sqlite`
- feature cache: `data/feature_cache/<version>/<key>.parquet`

## Why Research Reads Local Data Only

- deterministic reruns
- avoids API drift during analysis
- separates acquisition reliability from model logic
- supports offline / CI execution
