#!/usr/bin/env python3
"""
Fetch FinMind TaiwanStockInfoWithWarrantSummary by target stock id
and store linked warrant info into SQLite.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanStockInfoWithWarrantSummary"
CSV_FIELDS = [
    "stock_id",
    "target_stock_id",
    "type",
    "date",
    "end_date",
    "exercise_ratio",
    "fulfillment_price",
    "fulfillment_method",
    "fulfillment_start_date",
    "fulfillment_end_date",
    "close",
    "target_close",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch TaiwanStockInfoWithWarrantSummary and list warrants linked "
            "to a target stock id, then store into SQLite."
        )
    )
    parser.add_argument(
        "--stock-id",
        "--target-stock-id",
        dest="target_stock_id",
        required=True,
        help="Target stock ID, e.g. 2330",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="FinMind token. If omitted, read from FINMIND_SPONSOR_API_KEY in env/.env.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date YYYY-MM-DD for TaiwanStockInfoWithWarrantSummary.",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only keep warrants where end_date >= today.",
    )
    parser.add_argument(
        "--print-limit",
        type=int,
        default=0,
        help="How many warrant IDs to print. 0 means print all. Default: 0",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional CSV output path for detailed rows.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite output path. Default: <stock_id>.sqlite",
    )
    parser.add_argument(
        "--table-name",
        default="stock_warrant_summary",
        help="Target table name. Default: stock_warrant_summary",
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


def fetch_dataset(params: dict[str, str]) -> list[dict[str, Any]]:
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


def as_date(raw: Any) -> date | None:
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


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


def fetch_warrant_rows_with_start_date(
    target_stock_id: str, token: str, start_date: str | None
) -> list[dict[str, Any]]:
    params = {
        "dataset": DATASET,
        "data_id": target_stock_id,
        "token": token,
    }
    if start_date:
        params["start_date"] = start_date
    rows = fetch_dataset(params)
    return [row for row in rows if str(row.get("target_stock_id", "")).strip() == target_stock_id]


def filter_active(rows: list[dict[str, Any]], ref_date: date) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        end_dt = as_date(row.get("end_date"))
        if end_dt is not None and end_dt >= ref_date:
            result.append(row)
    return result


def get_unique_warrant_ids(rows: list[dict[str, Any]]) -> list[str]:
    ids = {
        str(row.get("stock_id", "")).strip()
        for row in rows
        if str(row.get("stock_id", "")).strip()
    }
    return sorted(ids)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in CSV_FIELDS})


def get_db_path(stock_id: str, cli_path: str | None) -> Path:
    return Path(cli_path) if cli_path else Path(f"{stock_id}.sqlite")


def ensure_tables(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_stock_id TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            warrant_type TEXT,
            date TEXT,
            end_date TEXT,
            exercise_ratio REAL,
            fulfillment_price REAL,
            fulfillment_method TEXT,
            fulfillment_start_date TEXT,
            fulfillment_end_date TEXT,
            close REAL,
            target_close REAL,
            inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (target_stock_id, stock_id, date)
        )
        """
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_target_stock" ON "{table_name}" (target_stock_id, stock_id)'
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
    target_stock_id: str,
    start_date: str | None,
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

        for row in rows:
            cur = conn.execute(
                f"""
                INSERT INTO "{table_name}" (
                    target_stock_id, stock_id, warrant_type, date, end_date, exercise_ratio,
                    fulfillment_price, fulfillment_method, fulfillment_start_date,
                    fulfillment_end_date, close, target_close
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_stock_id, stock_id, date) DO UPDATE SET
                    warrant_type = COALESCE(excluded.warrant_type, warrant_type),
                    end_date = COALESCE(excluded.end_date, end_date),
                    exercise_ratio = COALESCE(excluded.exercise_ratio, exercise_ratio),
                    fulfillment_price = COALESCE(excluded.fulfillment_price, fulfillment_price),
                    fulfillment_method = COALESCE(excluded.fulfillment_method, fulfillment_method),
                    fulfillment_start_date = COALESCE(excluded.fulfillment_start_date, fulfillment_start_date),
                    fulfillment_end_date = COALESCE(excluded.fulfillment_end_date, fulfillment_end_date),
                    close = COALESCE(excluded.close, close),
                    target_close = COALESCE(excluded.target_close, target_close)
                """,
                (
                    str(row.get("target_stock_id", target_stock_id)),
                    str(row.get("stock_id", "")),
                    as_text(row.get("type")),
                    as_text(row.get("date")),
                    as_text(row.get("end_date")),
                    as_float(row.get("exercise_ratio")),
                    as_float(row.get("fulfillment_price")),
                    as_text(row.get("fulfillment_method")),
                    as_text(row.get("fulfillment_start_date")),
                    as_text(row.get("fulfillment_end_date")),
                    as_float(row.get("close")),
                    as_float(row.get("target_close")),
                ),
            )
            inserted_rows += cur.rowcount

        conn.execute(
            """
            INSERT INTO fetch_history (
                dataset, stock_id, start_date, end_date, fetched_rows, inserted_rows
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (DATASET, target_stock_id, start_date or "", "", len(rows), inserted_rows),
        )
        conn.commit()
    finally:
        conn.close()

    return len(rows), inserted_rows


def main() -> int:
    args = parse_args()
    try:
        if args.print_limit < 0:
            raise ValueError("--print-limit must be greater than or equal to 0.")
        if args.start_date:
            parse_date(args.start_date)

        token = get_token(args.token)
        table_name = sanitize_table_name(args.table_name)
        rows = fetch_warrant_rows_with_start_date(
            target_stock_id=args.target_stock_id,
            token=token,
            start_date=args.start_date,
        )

        if args.active_only:
            rows = filter_active(rows, ref_date=date.today())

        warrant_ids = get_unique_warrant_ids(rows)
        total_rows = len(rows)
        total_unique = len(warrant_ids)
        print(f"Target stock_id: {args.target_stock_id}")
        print(f"Rows: {total_rows}")
        print(f"Unique warrant IDs: {total_unique}")

        db_path = get_db_path(stock_id=args.target_stock_id, cli_path=args.db_path)
        fetched, inserted = save_to_sqlite(
            rows=rows,
            target_stock_id=args.target_stock_id,
            start_date=args.start_date,
            db_path=db_path,
            table_name=table_name,
            replace=args.replace,
        )
        print(f"DB: {db_path}")
        print(f"Table: {table_name}")
        print(f"Fetched rows: {fetched}")
        print(f"Inserted rows: {inserted}")

        if args.output:
            output_path = Path(args.output)
            write_csv(output_path, rows)
            print(f"Detail CSV: {output_path}")

        limit = args.print_limit or total_unique
        print_count = min(limit, total_unique)
        if print_count > 0:
            print("Warrant IDs:")
            for warrant_id in warrant_ids[:print_count]:
                print(warrant_id)

        if args.print_limit and total_unique > args.print_limit:
            print(f"... truncated, add --print-limit 0 to print all {total_unique} IDs")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
