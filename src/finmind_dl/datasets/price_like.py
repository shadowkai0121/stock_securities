"""Shared ingestion routine for price-like datasets."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Callable

from finmind_dl.core.convert import as_int
from finmind_dl.core.date_utils import ensure_date_range, parse_iso_date
from finmind_dl.core.http_client import fetch_dataset
from finmind_dl.core.sqlite_store import open_connection, prepare_db_path
from finmind_dl.schema import init_schema

from .common import default_stock_db_path, fetch_trading_dates, summarize_result

Normalizer = Callable[[dict[str, Any]], tuple[Any, ...]]


def run_price_like(
    *,
    dataset: str,
    table_name: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    db_path: Path,
    replace: bool,
    token: str,
    normalizer: Normalizer,
    column_names: list[str],
) -> dict[str, Any]:
    start_dt = parse_iso_date(start_date, "--start-date")
    end_dt = parse_iso_date(end_date, "--end-date")
    ensure_date_range(start_dt, end_dt, start_name="--start-date", end_name="--end-date")

    raw_rows = fetch_dataset(
        dataset,
        token,
        {
            "data_id": stock_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    )
    trading_dates = fetch_trading_dates(token=token, start_date=start_date, end_date=end_date)

    by_date: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        date_str = str(row.get("date", "")).strip()
        if date_str:
            by_date[date_str] = row

    merged_rows: list[dict[str, Any]] = []
    for date_str in trading_dates:
        row = by_date.get(date_str)
        if row is None:
            merged_rows.append(
                {
                    "date": date_str,
                    "stock_id": stock_id,
                    "is_placeholder": 1,
                }
            )
        else:
            payload = dict(row)
            payload["is_placeholder"] = 0
            merged_rows.append(payload)

    prepare_db_path(db_path, replace=replace)
    conn = open_connection(db_path)
    inserted_rows = 0
    try:
        init_schema(conn)

        value_placeholders = ", ".join(["?"] * (2 + len(column_names) + 1))
        cols_sql = ", ".join(["date", "stock_id", *column_names, "is_placeholder"])
        set_sql = ",\n                    ".join(
            [f'{name} = COALESCE(excluded.{name}, "{table_name}".{name})' for name in column_names]
        )

        sql = f'''
            INSERT INTO "{table_name}" ({cols_sql})
            VALUES ({value_placeholders})
            ON CONFLICT(date, stock_id) DO UPDATE SET
                    {set_sql},
                    is_placeholder = CASE
                        WHEN excluded.is_placeholder = 0 THEN 0
                        ELSE "{table_name}".is_placeholder
                    END
        '''

        for row in merged_rows:
            normalized = normalizer(row)
            values = (
                str(row.get("date", "")),
                str(row.get("stock_id", stock_id)),
                *normalized,
                as_int(row.get("is_placeholder")) or 0,
            )
            cur = conn.execute(sql, values)
            inserted_rows += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    return summarize_result(
        dataset=dataset,
        table=table_name,
        stock_id=stock_id,
        query_mode="stock_range",
        start_date=start_date,
        end_date=end_date,
        db_path=db_path,
        fetched_rows=len(merged_rows),
        inserted_rows=inserted_rows,
    )


def resolve_stock_db(stock_id: str, cli_path: str | None) -> Path:
    return default_stock_db_path(stock_id, cli_path)
