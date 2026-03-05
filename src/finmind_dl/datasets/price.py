"""Handler for TaiwanStockPrice -> price_daily."""

from __future__ import annotations

from argparse import Namespace
from typing import Any

from finmind_dl.core.convert import as_float, as_int

from .price_like import resolve_stock_db, run_price_like

DATASET = "TaiwanStockPrice"
TABLE = "price_daily"


def _normalize(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        as_float(row.get("open")),
        as_float(row.get("max")),
        as_float(row.get("min")),
        as_float(row.get("close")),
        as_int(row.get("Trading_Volume")),
        as_int(row.get("Trading_money")),
        as_float(row.get("spread")),
        as_int(row.get("Trading_turnover")),
    )


def run(args: Namespace, token: str) -> dict[str, Any]:
    db_path = resolve_stock_db(args.stock_id, args.db_path)
    return run_price_like(
        dataset=DATASET,
        table_name=TABLE,
        stock_id=args.stock_id,
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=db_path,
        replace=bool(args.replace),
        token=token,
        normalizer=_normalize,
        column_names=[
            "open",
            "max",
            "min",
            "close",
            "trading_volume",
            "trading_money",
            "spread",
            "trading_turnover",
        ],
    )
