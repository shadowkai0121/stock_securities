"""Quality checks for locally ingested SQLite datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from data.storage.sqlite_store import SQLiteStore


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Result for a single quality check."""

    name: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    """Aggregated quality report for one dataset/table."""

    dataset_name: str
    table: str
    checks: tuple[CheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)


def check_table_exists(store: SQLiteStore, table: str) -> CheckResult:
    exists = store.table_exists(table)
    return CheckResult(
        name="table_exists",
        passed=exists,
        detail=f"table={table} exists={exists}",
    )


def check_row_count_positive(store: SQLiteStore, table: str, *, min_rows: int = 1) -> CheckResult:
    if not store.table_exists(table):
        return CheckResult(
            name="row_count_positive",
            passed=False,
            detail=f"table={table} missing",
        )
    count = store.row_count(table)
    ok = count >= int(min_rows)
    return CheckResult(
        name="row_count_positive",
        passed=ok,
        detail=f"row_count={count}, min_rows={min_rows}",
    )


def check_primary_key_unique(store: SQLiteStore, table: str, primary_keys: Sequence[str]) -> CheckResult:
    if not primary_keys:
        return CheckResult(name="primary_key_unique", passed=True, detail="no primary keys configured")
    if not store.table_exists(table):
        return CheckResult(name="primary_key_unique", passed=False, detail=f"table={table} missing")

    group_keys = ", ".join(f'"{key}"' for key in primary_keys)
    sql = (
        f'SELECT COUNT(*) FROM ('
        f'SELECT {group_keys}, COUNT(*) AS c FROM "{table}" '
        f'GROUP BY {group_keys} HAVING c > 1'
        f')'
    )
    with store.connect() as conn:
        duplicates = int(conn.execute(sql).fetchone()[0])
    return CheckResult(
        name="primary_key_unique",
        passed=duplicates == 0,
        detail=f"duplicate_key_groups={duplicates}",
    )


def check_non_null_columns(store: SQLiteStore, table: str, required_columns: Sequence[str]) -> CheckResult:
    if not required_columns:
        return CheckResult(name="required_non_null", passed=True, detail="no required columns configured")
    if not store.table_exists(table):
        return CheckResult(name="required_non_null", passed=False, detail=f"table={table} missing")

    failed_cols: list[str] = []
    with store.connect() as conn:
        for col in required_columns:
            sql = f'SELECT COUNT(*) FROM "{table}" WHERE "{col}" IS NULL OR TRIM(CAST("{col}" AS TEXT)) = ""'
            bad_count = int(conn.execute(sql).fetchone()[0])
            if bad_count > 0:
                failed_cols.append(f"{col}:{bad_count}")

    return CheckResult(
        name="required_non_null",
        passed=len(failed_cols) == 0,
        detail=("all required columns non-null" if not failed_cols else ", ".join(failed_cols)),
    )


def run_dataset_checks(
    *,
    store: SQLiteStore,
    dataset_name: str,
    table: str,
    primary_keys: Sequence[str],
    required_non_null_columns: Sequence[str],
    enabled_checks: Iterable[str],
) -> DataQualityReport:
    check_set = set(enabled_checks)
    checks: list[CheckResult] = []

    if "table_exists" in check_set:
        checks.append(check_table_exists(store, table))
    if "row_count_positive" in check_set:
        checks.append(check_row_count_positive(store, table))
    if "primary_key_unique" in check_set:
        checks.append(check_primary_key_unique(store, table, primary_keys))
    if "required_non_null" in check_set:
        checks.append(check_non_null_columns(store, table, required_non_null_columns))

    return DataQualityReport(dataset_name=dataset_name, table=table, checks=tuple(checks))
