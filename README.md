# finmind-dl

finmind api doc: `https://finmind.github.io/`

Unified FinMind downloader CLI for Taiwan stock datasets.

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
