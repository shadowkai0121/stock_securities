"""Common helpers for dataset handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from finmind_dl.core.convert import as_text
from finmind_dl.core.http_client import fetch_dataset

TRADING_DATE_DATASET = "TaiwanStockTradingDate"


def default_stock_db_path(stock_id: str, cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    return Path(f"{stock_id}.sqlite")


def default_market_db_path(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    return Path("market.sqlite")


def fetch_trading_dates(token: str, start_date: str, end_date: str) -> list[str]:
    rows = fetch_dataset(
        TRADING_DATE_DATASET,
        token,
        {
            "start_date": start_date,
            "end_date": end_date,
        },
    )
    unique_dates = {
        date_str
        for row in rows
        for date_str in [as_text(row.get("date"))]
        if date_str
    }
    return sorted(unique_dates)


def summarize_result(
    *,
    dataset: str,
    table: str,
    stock_id: str,
    query_mode: str,
    start_date: str | None,
    end_date: str | None,
    db_path: Path,
    fetched_rows: int,
    inserted_rows: int,
    extra_lines: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "dataset": dataset,
        "table": table,
        "stock_id": stock_id,
        "query_mode": query_mode,
        "start_date": start_date,
        "end_date": end_date,
        "db_path": db_path,
        "fetched_rows": fetched_rows,
        "inserted_rows": inserted_rows,
        "extra_lines": extra_lines or [],
    }
    if extra:
        payload.update(extra)
    return payload
