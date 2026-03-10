# Data Validation

`data_checks.py` provides reusable quality checks for ingested local datasets.

These checks are intentionally lightweight and designed for orchestration gates:

- table existence
- non-empty row counts
- primary key uniqueness
- required non-null fields
