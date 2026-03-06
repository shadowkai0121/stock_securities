"""Handler for TaiwanStockInfo -> stock_info."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Any

from finmind_dl.core.convert import as_text
from finmind_dl.core.date_utils import parse_iso_date
from finmind_dl.core.http_client import fetch_dataset
from finmind_dl.core.sqlite_store import open_connection, prepare_db_path
from finmind_dl.schema import init_schema

from .common import summarize_result

DATASET = "TaiwanStockInfo"
TABLE = "stock_info"


def _default_db_path(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    return Path("stock_info.sqlite")


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for row in rows:
        output.append(
            {
                "date": as_text(row.get("date")),
                "stock_id": as_text(row.get("stock_id")),
                "stock_name": as_text(row.get("stock_name")),
                "type": as_text(row.get("type")),
                "industry_category": as_text(row.get("industry_category")),
            }
        )
    return output


def run(args: Namespace, token: str) -> dict[str, Any]:
    if args.start_date:
        parse_iso_date(args.start_date, "--start-date")

    db_path = _default_db_path(args.db_path)
    params: dict[str, str] = {}
    if args.start_date:
        params["start_date"] = args.start_date

    raw_rows = fetch_dataset(DATASET, token, params)
    rows = _normalize_rows(raw_rows)

    prepare_db_path(db_path, replace=bool(args.replace))
    conn = open_connection(db_path)
    inserted_rows = 0
    try:
        init_schema(conn)
        for row in rows:
            cur = conn.execute(
                """
                INSERT INTO stock_info (date, stock_id, stock_name, type, industry_category)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date, stock_id, stock_name, type, industry_category) DO NOTHING
                """,
                (
                    row.get("date"),
                    row.get("stock_id"),
                    row.get("stock_name"),
                    row.get("type"),
                    row.get("industry_category"),
                ),
            )
            inserted_rows += cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return summarize_result(
        dataset=DATASET,
        table=TABLE,
        stock_id="__ALL__",
        query_mode="all_market_snapshot",
        start_date=args.start_date,
        end_date=None,
        db_path=db_path,
        fetched_rows=len(rows),
        inserted_rows=inserted_rows,
    )
