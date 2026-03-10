# Data Loaders

`finmind_loader.py` exposes a Python-native ingestion interface while preserving `finmind-dl` as the canonical downloader boundary.

Supported methods include:

- `download_price`
- `download_price_adj`
- `download_margin`
- `download_broker`
- `download_holding_shares`
- `download_stock_info`

These methods execute existing `finmind_dl` handlers (or CLI subprocess mode), rather than calling FinMind APIs directly from research modules.
