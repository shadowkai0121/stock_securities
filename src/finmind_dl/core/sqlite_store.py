"""SQLite path and connection helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def prepare_db_path(db_path: Path, *, replace: bool) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if replace and db_path.exists():
        db_path.unlink()


def open_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
