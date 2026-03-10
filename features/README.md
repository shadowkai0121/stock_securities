# Feature Layer

The feature layer computes reusable time-series and cross-sectional features from local datasets.

Modules:

- `feature_defs.py`: feature definitions and computation helpers
- `feature_store.py`: reproducible feature caching/versioning

Design goals:

- strategy-agnostic features
- reproducible cache keys
- local artifact persistence (parquet + metadata)
