"""Handler for TaiwanStockHoldingSharesPer -> holding_shares_per."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Any

from finmind_dl.core.convert import as_float, as_int, as_text
from finmind_dl.core.date_utils import ensure_date_range, parse_iso_date, to_iso
from finmind_dl.core.http_client import fetch_dataset
from finmind_dl.core.sqlite_store import open_connection, prepare_db_path
from finmind_dl.core.storage_layout import (
    ensure_market_db_layout,
    ensure_stock_db_layout,
    migrate_legacy_market_files,
)

from .common import default_market_db_path, default_stock_db_path, summarize_result

DATASET = "TaiwanStockHoldingSharesPer"
TABLE = "holding_shares_per"


def _resolve_mode(args: Namespace) -> tuple[str, str, str, dict[str, str], Path, str]:
    if args.all_market_date:
        if args.stock_id or args.start_date or args.end_date:
            raise ValueError(
                "--all-market-date cannot be combined with --stock-id/--start-date/--end-date."
            )
        market_date = to_iso(parse_iso_date(args.all_market_date, "--all-market-date"))
        return (
            "all_market_date",
            "__ALL__",
            market_date,
            {"start_date": market_date},
            default_market_db_path(args.db_path),
            market_date,
        )

    if not args.stock_id:
        raise ValueError("--stock-id is required when --all-market-date is not provided.")
    if not args.start_date or not args.end_date:
        raise ValueError("--start-date and --end-date are required in stock range mode.")

    start_dt = parse_iso_date(args.start_date, "--start-date")
    end_dt = parse_iso_date(args.end_date, "--end-date")
    ensure_date_range(start_dt, end_dt, start_name="--start-date", end_name="--end-date")

    return (
        "stock_range",
        args.stock_id,
        args.start_date,
        {
            "data_id": args.stock_id,
            "start_date": args.start_date,
            "end_date": args.end_date,
        },
        default_stock_db_path(args.stock_id, args.db_path),
        args.end_date,
    )


def run(args: Namespace, token: str) -> dict[str, Any]:
    query_mode, history_stock_id, start_date, params, db_path, end_date = _resolve_mode(args)

    rows = fetch_dataset(DATASET, token, params)

    if query_mode == "all_market_date" and not bool(args.replace):
        migrate_legacy_market_files(db_path)
    prepare_db_path(db_path, replace=bool(args.replace))
    conn = open_connection(db_path)
    inserted_rows = 0
    try:
        if query_mode == "all_market_date":
            ensure_market_db_layout(conn)
            for row in rows:
                cur = conn.execute(
                    """
                    INSERT INTO holding_shares_per (
                        date, stock_id, holding_shares_level, people, percent, unit, query_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, stock_id, holding_shares_level) DO UPDATE SET
                        people = COALESCE(excluded.people, holding_shares_per.people),
                        percent = COALESCE(excluded.percent, holding_shares_per.percent),
                        unit = COALESCE(excluded.unit, holding_shares_per.unit),
                        query_mode = excluded.query_mode
                    """,
                    (
                        as_text(row.get("date")) or "",
                        as_text(row.get("stock_id")) or "",
                        as_text(row.get("HoldingSharesLevel")) or "",
                        as_int(row.get("people")),
                        as_float(row.get("percent")),
                        as_int(row.get("unit")),
                        query_mode,
                    ),
                )
                inserted_rows += cur.rowcount
        else:
            ensure_stock_db_layout(conn, stock_id=history_stock_id)
            for row in rows:
                cur = conn.execute(
                    """
                    INSERT INTO holding_shares_per (
                        date, holding_shares_level, people, percent, unit, query_mode
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, holding_shares_level) DO UPDATE SET
                        people = COALESCE(excluded.people, holding_shares_per.people),
                        percent = COALESCE(excluded.percent, holding_shares_per.percent),
                        unit = COALESCE(excluded.unit, holding_shares_per.unit),
                        query_mode = excluded.query_mode
                    """,
                    (
                        as_text(row.get("date")) or "",
                        as_text(row.get("HoldingSharesLevel")) or "",
                        as_int(row.get("people")),
                        as_float(row.get("percent")),
                        as_int(row.get("unit")),
                        query_mode,
                    ),
                )
                inserted_rows += cur.rowcount
        conn.commit()
    finally:
        conn.close()

    return summarize_result(
        dataset=DATASET,
        table=TABLE,
        stock_id=history_stock_id,
        query_mode=query_mode,
        start_date=start_date,
        end_date=end_date,
        db_path=db_path,
        fetched_rows=len(rows),
        inserted_rows=inserted_rows,
        extra_lines=[f"Mode: {query_mode}"],
    )
