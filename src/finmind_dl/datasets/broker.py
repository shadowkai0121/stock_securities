"""Handler for TaiwanStockTradingDailyReport -> broker_trades."""

from __future__ import annotations

import sqlite3
from argparse import Namespace
from typing import Any

from finmind_dl.core.convert import as_float
from finmind_dl.core.date_utils import ensure_date_range, parse_iso_date
from finmind_dl.core.http_client import fetch_trading_daily_report
from finmind_dl.core.sqlite_store import open_connection, prepare_db_path
from finmind_dl.core.storage_layout import ensure_stock_db_layout

from .common import default_stock_db_path, fetch_trading_dates, summarize_result

DATASET = "TaiwanStockTradingDailyReport"
TABLE = "broker_trades"
NO_DATA_BROKER_ID = "__NO_DATA__"
NO_DATA_BROKER_NAME = "__NO_DATA__"


def run(args: Namespace, token: str) -> dict[str, Any]:
    start_dt = parse_iso_date(args.start_date, "--start-date")
    end_dt = parse_iso_date(args.end_date, "--end-date")
    ensure_date_range(start_dt, end_dt, start_name="--start-date", end_name="--end-date")

    stock_id = args.stock_id
    db_path = default_stock_db_path(stock_id, args.db_path)
    trading_dates = fetch_trading_dates(token=token, start_date=args.start_date, end_date=args.end_date)

    prepare_db_path(db_path, replace=bool(args.replace))
    conn = open_connection(db_path)
    fetched_rows = 0
    inserted_rows = 0
    touched_brokers: set[str] = set()
    try:
        ensure_stock_db_layout(conn, stock_id=stock_id)

        insert_sql = """
            INSERT INTO broker_trades (
                date, broker_id, broker_name, price, buy, sell, is_placeholder
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, broker_id, broker_name, price, buy, sell, is_placeholder)
            DO NOTHING
        """

        for idx, one_date in enumerate(trading_dates, start=1):
            rows = fetch_trading_daily_report(
                token,
                {
                    "data_id": stock_id,
                    "date": one_date,
                },
            )

            if rows:
                conn.execute(
                    """
                    DELETE FROM broker_trades
                    WHERE date = ?
                      AND broker_id = ?
                      AND broker_name = ?
                      AND is_placeholder = 1
                    """,
                    (one_date, NO_DATA_BROKER_ID, NO_DATA_BROKER_NAME),
                )

                for row in rows:
                    broker_id = str(row.get("securities_trader_id", "")).strip()
                    broker_name = str(row.get("securities_trader", "")).strip()
                    if not broker_id:
                        continue
                    fetched_rows += 1
                    cur = conn.execute(
                        insert_sql,
                        (
                            str(row.get("date", one_date)),
                            broker_id,
                            broker_name,
                            as_float(row.get("price")),
                            as_float(row.get("buy")),
                            as_float(row.get("sell")),
                            0,
                        ),
                    )
                    inserted_rows += cur.rowcount
                    touched_brokers.add(broker_id)
            else:
                fetched_rows += 1
                cur = conn.execute(
                    insert_sql,
                    (
                        one_date,
                        NO_DATA_BROKER_ID,
                        NO_DATA_BROKER_NAME,
                        None,
                        None,
                        None,
                        1,
                    ),
                )
                inserted_rows += cur.rowcount
                touched_brokers.add(NO_DATA_BROKER_ID)

            if idx % 20 == 0:
                conn.commit()

        conn.commit()
    finally:
        conn.close()

    return summarize_result(
        dataset=DATASET,
        table=TABLE,
        stock_id=stock_id,
        query_mode="stock_range",
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=db_path,
        fetched_rows=fetched_rows,
        inserted_rows=inserted_rows,
        extra_lines=[f"Touched brokers: {len(touched_brokers)}"],
        extra={"touched_brokers": len(touched_brokers)},
    )
