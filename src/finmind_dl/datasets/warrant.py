"""Handler for TaiwanStockInfoWithWarrantSummary -> warrant_summary."""

from __future__ import annotations

import csv
from argparse import Namespace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from finmind_dl.core.convert import as_float, as_text
from finmind_dl.core.date_utils import parse_iso_date
from finmind_dl.core.http_client import fetch_dataset
from finmind_dl.core.sqlite_store import open_connection, prepare_db_path
from finmind_dl.schema import init_schema

from .common import default_stock_db_path, summarize_result

DATASET = "TaiwanStockInfoWithWarrantSummary"
TABLE = "warrant_summary"
CSV_FIELDS = [
    "warrant_stock_id",
    "target_stock_id",
    "warrant_type",
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


def _parse_date_or_none(raw: Any) -> date | None:
    text = as_text(raw)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def filter_active_rows(rows: list[dict[str, Any]], ref_date: date) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        end_dt = _parse_date_or_none(row.get("end_date"))
        if end_dt and end_dt >= ref_date:
            result.append(row)
    return result


def _resolve_warrant_rows(target_stock_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("target_stock_id", "")).strip() != target_stock_id:
            continue
        result.append(
            {
                "target_stock_id": target_stock_id,
                "warrant_stock_id": str(row.get("stock_id", "")).strip(),
                "warrant_type": as_text(row.get("type")),
                "date": as_text(row.get("date")),
                "end_date": as_text(row.get("end_date")),
                "exercise_ratio": as_float(row.get("exercise_ratio")),
                "fulfillment_price": as_float(row.get("fulfillment_price")),
                "fulfillment_method": as_text(row.get("fulfillment_method")),
                "fulfillment_start_date": as_text(row.get("fulfillment_start_date")),
                "fulfillment_end_date": as_text(row.get("fulfillment_end_date")),
                "close": as_float(row.get("close")),
                "target_close": as_float(row.get("target_close")),
            }
        )
    return result


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in CSV_FIELDS})


def run(args: Namespace, token: str) -> dict[str, Any]:
    if args.start_date:
        parse_iso_date(args.start_date, "--start-date")
    if args.print_limit < 0:
        raise ValueError("--print-limit must be greater than or equal to 0.")

    target_stock_id = args.stock_id
    db_path = default_stock_db_path(target_stock_id, args.db_path)

    params = {"data_id": target_stock_id}
    if args.start_date:
        params["start_date"] = args.start_date
    raw_rows = fetch_dataset(DATASET, token, params)

    rows = _resolve_warrant_rows(target_stock_id, raw_rows)
    if args.active_only:
        rows = filter_active_rows(rows, ref_date=date.today())

    warrant_ids = sorted({row["warrant_stock_id"] for row in rows if row.get("warrant_stock_id")})

    prepare_db_path(db_path, replace=bool(args.replace))
    conn = open_connection(db_path)
    inserted_rows = 0
    try:
        init_schema(conn)
        for row in rows:
            cur = conn.execute(
                """
                INSERT INTO warrant_summary (
                    target_stock_id, warrant_stock_id, warrant_type, date, end_date,
                    exercise_ratio, fulfillment_price, fulfillment_method,
                    fulfillment_start_date, fulfillment_end_date, close, target_close
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_stock_id, warrant_stock_id, date) DO UPDATE SET
                    warrant_type = COALESCE(excluded.warrant_type, warrant_summary.warrant_type),
                    end_date = COALESCE(excluded.end_date, warrant_summary.end_date),
                    exercise_ratio = COALESCE(excluded.exercise_ratio, warrant_summary.exercise_ratio),
                    fulfillment_price = COALESCE(excluded.fulfillment_price, warrant_summary.fulfillment_price),
                    fulfillment_method = COALESCE(excluded.fulfillment_method, warrant_summary.fulfillment_method),
                    fulfillment_start_date = COALESCE(excluded.fulfillment_start_date, warrant_summary.fulfillment_start_date),
                    fulfillment_end_date = COALESCE(excluded.fulfillment_end_date, warrant_summary.fulfillment_end_date),
                    close = COALESCE(excluded.close, warrant_summary.close),
                    target_close = COALESCE(excluded.target_close, warrant_summary.target_close)
                """,
                (
                    row.get("target_stock_id"),
                    row.get("warrant_stock_id"),
                    row.get("warrant_type"),
                    row.get("date"),
                    row.get("end_date"),
                    row.get("exercise_ratio"),
                    row.get("fulfillment_price"),
                    row.get("fulfillment_method"),
                    row.get("fulfillment_start_date"),
                    row.get("fulfillment_end_date"),
                    row.get("close"),
                    row.get("target_close"),
                ),
            )
            inserted_rows += cur.rowcount
        conn.commit()
    finally:
        conn.close()

    if args.output_csv:
        _write_csv(Path(args.output_csv), rows)

    extra_lines = [
        f"Rows: {len(rows)}",
        f"Unique warrant IDs: {len(warrant_ids)}",
    ]
    if args.output_csv:
        extra_lines.append(f"Detail CSV: {args.output_csv}")

    print_count = len(warrant_ids) if args.print_limit == 0 else min(args.print_limit, len(warrant_ids))
    if print_count > 0:
        extra_lines.append("Warrant IDs:")
        extra_lines.extend(warrant_ids[:print_count])
        if args.print_limit and len(warrant_ids) > args.print_limit:
            extra_lines.append(
                f"... truncated, add --print-limit 0 to print all {len(warrant_ids)} IDs"
            )

    return summarize_result(
        dataset=DATASET,
        table=TABLE,
        stock_id=target_stock_id,
        query_mode="target_stock",
        start_date=args.start_date,
        end_date=None,
        db_path=db_path,
        fetched_rows=len(rows),
        inserted_rows=inserted_rows,
        extra_lines=extra_lines,
        extra={"warrant_ids": warrant_ids},
    )
