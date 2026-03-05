#!/usr/bin/env python3
"""
Fetch FinMind TaiwanStockHoldingSharesPer data and store it into SQLite.

Supported query modes:
1) Stock range mode:
   --stock-id 2330 --start-date 2020-01-01 --end-date 2026-03-05
2) All-market single-date mode (backer/sponsor):
   --start-date 2026-02-26
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockHoldingSharesPer"
ALL_MARKET_STOCK_ID = "__ALL__"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch TaiwanStockHoldingSharesPer and store into SQLite."
    )
    parser.add_argument(
        "--stock-id",
        default=None,
        help="Stock ID, e.g. 2330. Omit for all-market single-date mode.",
    )
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date YYYY-MM-DD. Required only when --stock-id is set.",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="FinMind token. If omitted, read from FINMIND_SPONSOR_API_KEY in env/.env.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite output path. Default: <stock_id>.sqlite or holding_shares_per.sqlite",
    )
    parser.add_argument(
        "--table-name",
        default="stock_holding_shares_per",
        help="Target table name. Default: stock_holding_shares_per",
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


def sanitize_table_name(value: str) -> str:
    candidate = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in value.strip())
    candidate = candidate.strip("_")
    if not candidate:
        raise ValueError("Invalid --table-name: must contain at least one valid character.")
    return candidate.lower()


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


def as_int(raw: Any) -> int | None:
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None
    return int(float(text))


def as_float(raw: Any) -> float | None:
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None
    return float(text)


def as_text(raw: Any) -> str | None:
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None
    return text


def fetch_dataset(params: dict[str, str]) -> list[dict[str, Any]]:
    request_url = f"{API_URL}?{urlencode(params)}"
    try:
        with urlopen(request_url, timeout=60) as response:
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


def fetch_rows(
    stock_id: str | None,
    start_date: str,
    end_date: str | None,
    token: str,
) -> tuple[list[dict[str, Any]], str]:
    params: dict[str, str] = {
        "dataset": DATASET,
        "start_date": start_date,
        "token": token,
    }
    if stock_id:
        params["data_id"] = stock_id
        if end_date:
            params["end_date"] = end_date
        query_mode = "stock_range"
    else:
        query_mode = "all_market_date"
    rows = fetch_dataset(params)
    return rows, query_mode


def get_db_path(stock_id: str | None, cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    if stock_id:
        return Path(f"{stock_id}.sqlite")
    return Path("holding_shares_per.sqlite")


def ensure_tables(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            HoldingSharesLevel TEXT NOT NULL,
            people INTEGER,
            percent REAL,
            unit INTEGER,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (date, stock_id, HoldingSharesLevel)
        )
        """
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_date_stock" ON "{table_name}" (date, stock_id)'
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset TEXT NOT NULL DEFAULT '',
            stock_id TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            fetched_rows INTEGER NOT NULL,
            inserted_rows INTEGER NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(fetch_history)")}
    if "dataset" not in cols:
        conn.execute(
            "ALTER TABLE fetch_history ADD COLUMN dataset TEXT NOT NULL DEFAULT ''"
        )


def save_to_sqlite(
    rows: list[dict[str, Any]],
    stock_id: str | None,
    start_date: str,
    end_date: str,
    db_path: Path,
    table_name: str,
    replace: bool,
) -> tuple[int, int]:
    if replace and db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    inserted_rows = 0
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        ensure_tables(conn, table_name)

        for idx, row in enumerate(rows, start=1):
            cur = conn.execute(
                f"""
                INSERT INTO "{table_name}" (
                    date, stock_id, HoldingSharesLevel, people, percent, unit
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, stock_id, HoldingSharesLevel) DO UPDATE SET
                    people = COALESCE(excluded.people, people),
                    percent = COALESCE(excluded.percent, percent),
                    unit = COALESCE(excluded.unit, unit)
                """,
                (
                    as_text(row.get("date")) or "",
                    as_text(row.get("stock_id")) or "",
                    as_text(row.get("HoldingSharesLevel")) or "",
                    as_int(row.get("people")),
                    as_float(row.get("percent")),
                    as_int(row.get("unit")),
                ),
            )
            inserted_rows += cur.rowcount

            if idx % 20000 == 0:
                conn.commit()

        conn.execute(
            """
            INSERT INTO fetch_history (
                dataset, stock_id, start_date, end_date, fetched_rows, inserted_rows
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                DATASET,
                stock_id or ALL_MARKET_STOCK_ID,
                start_date,
                end_date,
                len(rows),
                inserted_rows,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return len(rows), inserted_rows


def main() -> int:
    args = parse_args()
    try:
        start_dt = parse_date(args.start_date)
        if args.stock_id:
            end_date = args.end_date or args.start_date
            end_dt = parse_date(end_date)
            if end_dt < start_dt:
                raise ValueError("--end-date must be greater than or equal to --start-date.")
        else:
            if args.end_date:
                raise ValueError("--end-date is only allowed when --stock-id is set.")
            end_date = args.start_date

        token = get_token(args.token)
        table_name = sanitize_table_name(args.table_name)
        rows, query_mode = fetch_rows(
            stock_id=args.stock_id,
            start_date=args.start_date,
            end_date=end_date if args.stock_id else None,
            token=token,
        )
        db_path = get_db_path(stock_id=args.stock_id, cli_path=args.db_path)
        fetched, inserted = save_to_sqlite(
            rows=rows,
            stock_id=args.stock_id,
            start_date=args.start_date,
            end_date=end_date,
            db_path=db_path,
            table_name=table_name,
            replace=args.replace,
        )
        print(f"Mode: {query_mode}")
        print(f"DB: {db_path}")
        print(f"Table: {table_name}")
        print(f"Fetched rows: {fetched}")
        print(f"Inserted rows: {inserted}")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
