"""Structured SQLite read layer used by research modules."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd


@dataclass(frozen=True, slots=True)
class TableStats:
    """Basic table statistics for quick health checks."""

    table: str
    row_count: int
    min_date: str | None
    max_date: str | None


class SQLiteStore:
    """Thin typed wrapper around a local SQLite database file."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite DB not found: {self.db_path}")
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def list_tables(self) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def table_exists(self, table: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
                (table,),
            ).fetchone()
        return row is not None

    def row_count(self, table: str, where: str | None = None, params: Sequence[Any] | None = None) -> int:
        sql = f'SELECT COUNT(*) FROM "{table}"'
        if where:
            sql += f" WHERE {where}"
        with self.connect() as conn:
            count = conn.execute(sql, tuple(params or ())).fetchone()[0]
        return int(count)

    def read_table(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        params: Sequence[Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        cols = ", ".join(columns) if columns else "*"
        sql = f'SELECT {cols} FROM "{table}"'
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        with self.connect() as conn:
            return pd.read_sql_query(sql, conn, params=list(params or ()))

    def min_max_date(self, table: str, *, date_col: str = "date") -> tuple[str | None, str | None]:
        sql = f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"'
        with self.connect() as conn:
            row = conn.execute(sql).fetchone()
        if row is None:
            return (None, None)
        return (row[0], row[1])

    def table_stats(self, table: str, *, date_col: str = "date") -> TableStats:
        count = self.row_count(table)
        min_date, max_date = self.min_max_date(table, date_col=date_col)
        return TableStats(table=table, row_count=count, min_date=min_date, max_date=max_date)

    def ensure_tables(self, required_tables: Sequence[str]) -> list[str]:
        """Return missing tables from ``required_tables``."""

        missing: list[str] = []
        existing = set(self.list_tables())
        for table in required_tables:
            if table not in existing:
                missing.append(table)
        return missing
