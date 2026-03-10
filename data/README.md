# Data Layer

This directory is the official data architecture for the research platform.

Flow:

1. FinMind API
2. `finmind-dl` ingestion tool (`src/finmind_dl`)
3. Local SQLite persistence (`data/*.sqlite` or custom paths)
4. Research-facing loaders (`research/data_loader.py`)

The `finmind-dl` tool is preserved as a first-class ingestion boundary. Research modules must not call remote APIs directly.

## Structure

- `catalog/`: dataset catalog and ingestion metadata
- `loaders/`: ingestion orchestrators/wrappers around `finmind-dl`
- `storage/`: local SQLite access abstractions
- `validation/`: dataset quality checks

## Notes

- SQLite files can still be stored directly under `data/` (e.g., `data/2330.sqlite`).
- Python helper modules live under subdirectories and do not replace the downloader CLI.
