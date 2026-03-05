"""Handler for TaiwanStockMarginPurchaseShortSale -> margin_daily."""

from __future__ import annotations

from argparse import Namespace
from typing import Any

from finmind_dl.core.convert import as_int, as_text

from .price_like import resolve_stock_db, run_price_like

DATASET = "TaiwanStockMarginPurchaseShortSale"
TABLE = "margin_daily"


def _normalize(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        as_int(row.get("MarginPurchaseBuy")),
        as_int(row.get("MarginPurchaseCashRepayment")),
        as_int(row.get("MarginPurchaseLimit")),
        as_int(row.get("MarginPurchaseSell")),
        as_int(row.get("MarginPurchaseTodayBalance")),
        as_int(row.get("MarginPurchaseYesterdayBalance")),
        as_int(row.get("OffsetLoanAndShort")),
        as_int(row.get("ShortSaleBuy")),
        as_int(row.get("ShortSaleCashRepayment")),
        as_int(row.get("ShortSaleLimit")),
        as_int(row.get("ShortSaleSell")),
        as_int(row.get("ShortSaleTodayBalance")),
        as_int(row.get("ShortSaleYesterdayBalance")),
        as_text(row.get("Note")),
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
            "margin_purchase_buy",
            "margin_purchase_cash_repayment",
            "margin_purchase_limit",
            "margin_purchase_sell",
            "margin_purchase_today_balance",
            "margin_purchase_yesterday_balance",
            "offset_loan_and_short",
            "short_sale_buy",
            "short_sale_cash_repayment",
            "short_sale_limit",
            "short_sale_sell",
            "short_sale_today_balance",
            "short_sale_yesterday_balance",
            "note",
        ],
    )
