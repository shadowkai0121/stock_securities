#!/usr/bin/env python3
"""
Fetch FinMind TaiwanStockTradingDailyReport data and store it into SQLite.

DB layout:
- One SQLite file per stock id (default: <stock_id>.sqlite)
- One table per broker id (table name: broker_<securities_trader_id>)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockTradingDailyReport"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch broker trading data and store into stock-id SQLite DB."
    )
    parser.add_argument("--stock-id", required=True, help="Stock ID, e.g. 8271.")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD.")
    parser.add_argument(
        "--token",
        default=None,
        help="FinMind token. If omitted, read from FINMIND_SPONSOR_API_KEY in env/.env.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite output path. Default: <stock_id>.sqlite",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete target SQLite file before writing.",
    )
    return parser.parse_args()


def parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env_map: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key.strip()] = value.strip().strip("'").strip('"')
    return env_map


def get_token(cli_token: str | None) -> str:
    if cli_token:
        return cli_token
    env_map = load_env_file(Path(".env"))
    token = (
        os.getenv("FINMIND_SPONSOR_API_KEY")
        or env_map.get("FINMIND_SPONSOR_API_KEY")
        or os.getenv("FINMIND_TOKEN")
        or env_map.get("FINMIND_TOKEN")
    )
    if not token:
        raise RuntimeError(
            "Missing FinMind token. Set --token or FINMIND_SPONSOR_API_KEY in env/.env."
        )
    return token


def fetch_one_day(stock_id: str, date_str: str, token: str) -> list[dict[str, Any]]:
    params = {
        "dataset": DATASET,
        "data_id": stock_id,
        "start_date": date_str,
        "token": token,
    }
    request_url = f"{API_URL}?{urlencode(params)}"
    try:
        with urlopen(request_url, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error: {exc.reason}") from exc

    data = payload.get("data")
    if not isinstance(data, list):
        msg = payload.get("msg") or payload.get("message") or "Unknown API response."
        raise RuntimeError(f"Invalid FinMind response: {msg}")
    return data


def fetch_range(stock_id: str, start_date: str, end_date: str, token: str) -> list[dict[str, Any]]:
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    rows: list[dict[str, Any]] = []
    cur = start_dt
    while cur <= end_dt:
        one_date = cur.strftime("%Y-%m-%d")
        rows.extend(fetch_one_day(stock_id=stock_id, date_str=one_date, token=token))
        cur += timedelta(days=1)
    return rows


def get_db_path(stock_id: str, cli_path: str | None) -> Path:
    return Path(cli_path) if cli_path else Path(f"{stock_id}.sqlite")


def broker_table_name(broker_id: str) -> str:
    raw_id = broker_id.strip()
    safe_id = re.sub(r"[^0-9A-Za-z_]+", "_", raw_id).strip("_").lower()
    if not safe_id:
        safe_id = "unknown"
    digest = hashlib.sha1(raw_id.encode("utf-8")).hexdigest()[:10]
    return f"broker_{safe_id}_{digest}"


def ensure_meta_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_tables (
            securities_trader_id TEXT PRIMARY KEY,
            securities_trader TEXT NOT NULL,
            table_name TEXT NOT NULL UNIQUE,
            stock_id TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            fetched_rows INTEGER NOT NULL,
            inserted_rows INTEGER NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def ensure_broker_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            securities_trader_id TEXT NOT NULL,
            securities_trader TEXT NOT NULL,
            price REAL NOT NULL,
            buy REAL NOT NULL,
            sell REAL NOT NULL,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (
                date, stock_id, securities_trader_id, securities_trader, price, buy, sell
            )
        )
        """
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date" ON "{table_name}" (date)'
    )


def as_float(raw: Any) -> float:
    text = "" if raw is None else str(raw).strip()
    if not text:
        return 0.0
    return float(text)


def save_to_sqlite(
    rows: list[dict[str, Any]],
    stock_id: str,
    start_date: str,
    end_date: str,
    db_path: Path,
    replace: bool,
) -> tuple[int, int, int]:
    if replace and db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    inserted_rows = 0
    touched_brokers: set[str] = set()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        ensure_meta_tables(conn)

        for row in rows:
            broker_id = str(row.get("securities_trader_id", "")).strip()
            broker_name = str(row.get("securities_trader", "")).strip()
            if not broker_id:
                continue

            table_name = broker_table_name(broker_id)
            ensure_broker_table(conn, table_name)
            conn.execute(
                """
                INSERT INTO broker_tables (
                    securities_trader_id, securities_trader, table_name, stock_id, updated_at
                )
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(securities_trader_id) DO UPDATE SET
                    securities_trader=excluded.securities_trader,
                    table_name=excluded.table_name,
                    stock_id=excluded.stock_id,
                    updated_at=datetime('now')
                """,
                (broker_id, broker_name, table_name, stock_id),
            )

            cur = conn.execute(
                f"""
                INSERT OR IGNORE INTO "{table_name}" (
                    date, stock_id, securities_trader_id, securities_trader, price, buy, sell
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row.get("date", "")),
                    str(row.get("stock_id", stock_id)),
                    broker_id,
                    broker_name,
                    as_float(row.get("price")),
                    as_float(row.get("buy")),
                    as_float(row.get("sell")),
                ),
            )
            inserted_rows += cur.rowcount
            touched_brokers.add(broker_id)

        conn.execute(
            """
            INSERT INTO fetch_history (stock_id, start_date, end_date, fetched_rows, inserted_rows)
            VALUES (?, ?, ?, ?, ?)
            """,
            (stock_id, start_date, end_date, len(rows), inserted_rows),
        )
        conn.commit()
    finally:
        conn.close()

    return len(rows), inserted_rows, len(touched_brokers)


def main() -> int:
    args = parse_args()
    try:
        start_dt = parse_date(args.start_date)
        end_dt = parse_date(args.end_date)
        if end_dt < start_dt:
            raise ValueError("--end-date must be greater than or equal to --start-date.")

        token = get_token(args.token)
        rows = fetch_range(
            stock_id=args.stock_id,
            start_date=args.start_date,
            end_date=args.end_date,
            token=token,
        )
        db_path = get_db_path(stock_id=args.stock_id, cli_path=args.db_path)
        fetched, inserted, brokers = save_to_sqlite(
            rows=rows,
            stock_id=args.stock_id,
            start_date=args.start_date,
            end_date=args.end_date,
            db_path=db_path,
            replace=args.replace,
        )
        print(f"DB: {db_path}")
        print(f"Fetched rows: {fetched}")
        print(f"Inserted rows: {inserted}")
        print(f"Touched broker tables: {brokers}")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
