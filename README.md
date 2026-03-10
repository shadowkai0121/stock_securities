# finmind-dl + Quant Research Platform

finmind api doc: `https://finmind.github.io/`

This repository now combines:

- `finmind-dl`: the canonical data ingestion CLI/tooling layer
- a local-data-first quantitative research platform with orchestrated experiment runs

`finmind-dl` remains a first-class component and is the official ingestion interface.

## Data Architecture

Canonical flow:

`FinMind API -> finmind-dl -> SQLite -> research data loader -> universe -> feature store -> strategy -> backtest -> statistics -> experiment registry -> report`

Research code should read local persisted datasets only, not remote APIs.

## New Platform Entrypoints

- Python ingestion wrapper: `data/loaders/finmind_loader.py`
- Research orchestrator: `research/orchestrator.py`
- Experiment registry: `experiments/registry.py`
- Example end-to-end run: `experiments/example_ma_cross/run_experiment.py`

## Quickstart (Platform)

```bash
make install
make download-sample-data
make run-example-experiment
```

Or run directly:

```bash
python experiments/example_ma_cross/run_experiment.py --config experiments/example_ma_cross/config.json
```

If local data is missing, set token first:

```bash
set FINMIND_SPONSOR_API_KEY=your_token_here
```

## Install

```bash
python -m pip install -e .
```

After install:

```bash
finmind-dl --help
```

## Token Resolution Order

`finmind-dl` reads token in this order:

1. `--token`
2. `FINMIND_SPONSOR_API_KEY`
3. `FINMIND_TOKEN`
4. `.env` with the same keys

Example `.env`:

```env
FINMIND_SPONSOR_API_KEY=your_token
```

## Commands

### daily (一次下載指定標的日頻資料)

```bash
finmind-dl daily --stock-id 2330 --start-date 2026-01-01 --end-date 2026-03-01
```

預設會依序下載並寫入同一個 DB：
- `price_daily`
- `price_adj_daily`
- `margin_daily`
- `broker_trades`

若要同時包含股權分散資料：

```bash
finmind-dl daily --stock-id 2330 --start-date 2026-01-01 --end-date 2026-03-01 --include-holding-shares
```

### price

```bash
finmind-dl price --stock-id 2330 --start-date 2026-01-01 --end-date 2026-03-01
```

### price-adj

```bash
finmind-dl price-adj --stock-id 2330 --start-date 2026-01-01 --end-date 2026-03-01
```

### margin

```bash
finmind-dl margin --stock-id 2330 --start-date 2026-01-01 --end-date 2026-03-01
```

### broker

```bash
finmind-dl broker --stock-id 8271 --start-date 2026-02-26 --end-date 2026-03-03
```

### warrant

```bash
finmind-dl warrant --stock-id 2330 --start-date 2020-04-06 --active-only --print-limit 50
```

Export warrant details to CSV:

```bash
finmind-dl warrant --stock-id 2330 --output-csv outputs/2330_warrants.csv
```

### holding-shares (stock range mode)

```bash
finmind-dl holding-shares --stock-id 2330 --start-date 2020-01-01 --end-date 2026-03-05
```

### holding-shares (all-market single date)

```bash
finmind-dl holding-shares --all-market-date 2026-03-05
```

### stock-info (all-market stock/industry mapping)

```bash
finmind-dl stock-info --start-date 2019-01-01 --db-path data/stock_info.sqlite
```

## Default DB Output Rules

- Commands with `--stock-id`: `<stock_id>.sqlite`
- `holding-shares --all-market-date`: `holding_shares_per.sqlite`
- `--db-path` overrides default.

## Exit Codes

- `0`: success
- `2`: validation/argument error
- `3`: API/network error
- `4`: database write/runtime error

## SQLite Tables

- `meta_runs`
- `price_daily`
- `price_adj_daily`
- `margin_daily`
- `broker_trades`
- `warrant_summary`
- `holding_shares_per`
- `stock_info`

## Compatibility Note

This refactor changed downloader schema and removed legacy standalone scripts.

- `sqlite_broker_web.py`
- `research/broker_flow.py`

still assume old broker-table layout and are **not yet migrated**.

## API Reference Used

Official docs checked during implementation:

- https://finmind.github.io/
- https://finmind.github.io/tutor/TaiwanMarket/Chip/
- https://finmind.github.io/tutor/TaiwanMarket/Technical/

## Thesis Validation Notebook

Replication assets for the biotech broker-flow thesis are under `papers/`:

- `papers/thesis_biotech_validation.ipynb`
- `papers/thesis_biotech_validation_config.json`
- `papers/thesis_biotech_validation_utils.py`

You can run it interactively in Jupyter, or headless:

```bash
python -m jupyter nbconvert --to notebook --execute papers/thesis_biotech_validation.ipynb --output papers/thesis_biotech_validation.executed.ipynb
```

Default output files are written into `papers/` (report + CSV summaries).

## MA Cross Thesis Pipeline

Industry-level MA cross thesis batch pipeline:

```bash
python strategies/ma-cross/thesis_pipeline.py
```

This will:
- download `price_adj_daily` into `data/<stock_id>.sqlite`
- generate thesis outputs under `strategies/ma-cross/outputs/thesis/`

## Additional Documentation

- system overview: `docs/architecture/system_overview.md`
- data flow: `docs/architecture/data_flow.md`
- research pipeline: `docs/research-workflow/research_pipeline.md`
- experiment lifecycle: `docs/research-workflow/experiment_lifecycle.md`
- data catalog docs: `docs/data-catalog/`
