# MA Cross Backtest

This folder contains a standalone moving-average crossover backtest (long/cash only).

## Files

- `backtest.py`: strategy CLI script.
- `test_backtest.py`: unit tests.
- `PLAN.md`: implementation plan and scope.

## Usage

Run from repo root:

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2024-01-01 \
  --end-date 2025-12-31
```

### Key Arguments

- `--short-window` (default: `20`)
- `--long-window` (default: `60`, must be greater than short)
- `--table` (`price_adj_daily` or `price_daily`, default: `price_adj_daily`)
- `--db-path` (default: `data/<stock_id>.sqlite`)
- `--ensure-data` (fetch data from FinMind before running)
- `--replace-db` (only used with `--ensure-data`)
- `--token` (optional FinMind token; env/.env fallback supported)
- `--fee-bps` (default: `0.0`, cost per position-change event)
- `--output-dir` (default: `strategies/ma-cross/outputs`)
- `--no-plot` (skip `plot.png`)

### Example With Auto Fetch

```bash
python strategies/ma-cross/backtest.py \
  --stock-id 2330 \
  --start-date 2024-01-01 \
  --end-date 2025-12-31 \
  --ensure-data \
  --table price_adj_daily
```

## Strategy Definition

- SMA crossover signal:
  - `signal = 1` when `short_ma > long_ma`
  - `signal = 0` otherwise
- Warm-up periods (NaN MA) force `signal = 0`
- Position uses one-day lag to avoid lookahead:
  - `position = signal.shift(1)`
- Return model:
  - `ret = close.pct_change()`
  - `strategy_ret = position * ret - cost`
  - `cost = signal.diff().abs().shift(1) * fee_bps / 10000`

## Data Requirements

SQLite table columns needed:

- `date`
- `stock_id`
- `open`
- `close`
- `is_placeholder`

Rows are filtered by:

- target `stock_id`
- date range
- `is_placeholder == 0`
- non-null `close`

## Outputs

Output path pattern:

`<output-dir>/<stock>_<start>_<end>_s<short>_l<long>_<table>/`

Generated files:

- `equity.csv`
- `trades.csv`
- `report.json`
- `plot.png` (unless `--no-plot`)

Console summary includes:

- date range and MA params
- trade count
- total return
- buy-hold return
- CAGR
- max drawdown
- annualized volatility
- Sharpe (rf=0)

## Run Tests

```bash
python -m unittest discover -s strategies/ma-cross -p "test_backtest.py"
```

## Thesis Draft (Industry Validation)

- `THESIS.md`
- `THESIS_APPENDIX.md`
- `FIGURE_TABLE_SPECS.md`

## Integration With New Research Pipeline

This strategy is now integrated into the standardized research stack via:

- `research/strategies/ma_cross_adapter.py`

The adapter preserves existing MA-cross signal behavior while making it runnable through:

- `research/orchestrator.py`
- `experiments/example_ma_cross/run_experiment.py`
