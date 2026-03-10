# Universe Layer

`universe_builder.py` constructs date-aware Taiwan equity research universes from locally persisted metadata and price history.

Supported filters:

- TWSE/TPEX common equity focus
- optional ETF exclusion
- optional warrant exclusion
- inactive/delisted proxy filter using recent trading activity
- liquidity threshold filter
- minimum history-day requirement

Output format:

- `date`
- `stock_id`
- `tradable_flag` (`0` or `1`)
