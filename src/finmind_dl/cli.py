"""Unified command line interface for FinMind download tasks."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

from finmind_dl.core.config import resolve_token
from finmind_dl.core.history import build_requested_params, new_run_id, try_log_meta_run
from finmind_dl.core.http_client import APIError
from finmind_dl.datasets import broker, daily, holding_shares, margin, price, price_adj, warrant


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--token",
        default=None,
        help=(
            "FinMind token. If omitted, read FINMIND_SPONSOR_API_KEY or "
            "FINMIND_TOKEN from env/.env."
        ),
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite output path. Command default applies when omitted.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete target SQLite file before writing.",
    )


def _add_stock_range_subcommand(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    name: str,
    description: str,
    handler: Any,
) -> None:
    parser = subparsers.add_parser(name, help=description, description=description)
    parser.add_argument("--stock-id", required=True, help="Stock ID, e.g. 2330")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
    _add_common_args(parser)
    parser.set_defaults(command=name, handler=handler)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="finmind-dl",
        description="Unified FinMind downloader CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_stock_range_subcommand(
        subparsers,
        name="price",
        description="Fetch TaiwanStockPrice into price_daily",
        handler=price.run,
    )
    _add_stock_range_subcommand(
        subparsers,
        name="price-adj",
        description="Fetch TaiwanStockPriceAdj into price_adj_daily",
        handler=price_adj.run,
    )
    _add_stock_range_subcommand(
        subparsers,
        name="margin",
        description="Fetch TaiwanStockMarginPurchaseShortSale into margin_daily",
        handler=margin.run,
    )
    _add_stock_range_subcommand(
        subparsers,
        name="broker",
        description="Fetch TaiwanStockTradingDailyReport into broker_trades",
        handler=broker.run,
    )
    daily_parser = subparsers.add_parser(
        "daily",
        help="One-shot download daily stock datasets into one DB",
        description=(
            "One-shot download by stock id. Includes price, price-adj, margin, broker "
            "and optional holding-shares."
        ),
    )
    daily_parser.add_argument("--stock-id", required=True, help="Stock ID, e.g. 2330")
    daily_parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    daily_parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
    daily_parser.add_argument(
        "--include-holding-shares",
        action="store_true",
        help="Also fetch TaiwanStockHoldingSharesPer in stock-range mode.",
    )
    _add_common_args(daily_parser)
    daily_parser.set_defaults(command="daily", handler=daily.run)

    warrant_parser = subparsers.add_parser(
        "warrant",
        help="Fetch TaiwanStockInfoWithWarrantSummary into warrant_summary",
        description="Fetch TaiwanStockInfoWithWarrantSummary into warrant_summary",
    )
    warrant_parser.add_argument("--stock-id", required=True, help="Target stock ID, e.g. 2330")
    warrant_parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date YYYY-MM-DD",
    )
    warrant_parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only keep warrants where end_date >= today.",
    )
    warrant_parser.add_argument(
        "--print-limit",
        default=0,
        type=int,
        help="How many warrant IDs to print. 0 means print all.",
    )
    warrant_parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional CSV output path for warrant detail rows.",
    )
    _add_common_args(warrant_parser)
    warrant_parser.set_defaults(command="warrant", handler=warrant.run)

    hold_parser = subparsers.add_parser(
        "holding-shares",
        help="Fetch TaiwanStockHoldingSharesPer into holding_shares_per",
        description="Fetch TaiwanStockHoldingSharesPer into holding_shares_per",
    )
    hold_parser.add_argument("--stock-id", default=None, help="Stock ID for stock range mode")
    hold_parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    hold_parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD")
    hold_parser.add_argument(
        "--all-market-date",
        default=None,
        help="All-market single date mode (YYYY-MM-DD).",
    )
    _add_common_args(hold_parser)
    hold_parser.set_defaults(command="holding-shares", handler=holding_shares.run)

    return parser


def _fallback_context(args: argparse.Namespace) -> dict[str, Any]:
    command = getattr(args, "command", "")

    if command == "price":
        return {
            "dataset": price.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "stock_range",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "price-adj":
        return {
            "dataset": price_adj.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "stock_range",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "margin":
        return {
            "dataset": margin.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "stock_range",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "broker":
        return {
            "dataset": broker.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "stock_range",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "daily":
        return {
            "dataset": daily.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "stock_range_bundle",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "warrant":
        return {
            "dataset": warrant.DATASET,
            "stock_id": getattr(args, "stock_id", "__UNKNOWN__"),
            "query_mode": "target_stock",
            "start_date": getattr(args, "start_date", None),
            "end_date": None,
            "db_path": Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{getattr(args, 'stock_id', '__UNKNOWN__')}.sqlite"),
        }
    if command == "holding-shares":
        all_market_date = getattr(args, "all_market_date", None)
        if all_market_date:
            db_path = Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path("holding_shares_per.sqlite")
            return {
                "dataset": holding_shares.DATASET,
                "stock_id": "__ALL__",
                "query_mode": "all_market_date",
                "start_date": all_market_date,
                "end_date": all_market_date,
                "db_path": db_path,
            }
        stock_id = getattr(args, "stock_id", "__UNKNOWN__")
        db_path = Path(getattr(args, "db_path")) if getattr(args, "db_path", None) else Path(f"{stock_id}.sqlite")
        return {
            "dataset": holding_shares.DATASET,
            "stock_id": stock_id,
            "query_mode": "stock_range",
            "start_date": getattr(args, "start_date", None),
            "end_date": getattr(args, "end_date", None),
            "db_path": db_path,
        }

    return {
        "dataset": "unknown",
        "stock_id": "__UNKNOWN__",
        "query_mode": "unknown",
        "start_date": None,
        "end_date": None,
        "db_path": None,
    }


def _print_success(result: dict[str, Any]) -> None:
    print(f"Dataset: {result['dataset']}")
    print(f"DB: {result['db_path']}")
    print(f"Table: {result['table']}")
    print(f"Fetched rows: {result['fetched_rows']}")
    print(f"Inserted rows: {result['inserted_rows']}")
    for line in result.get("extra_lines", []):
        print(line)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)

    run_id = new_run_id()
    requested_params_json = build_requested_params(args)
    fallback = _fallback_context(args)

    try:
        token = resolve_token(args.token)
        result = args.handler(args, token)
        try_log_meta_run(
            Path(result["db_path"]),
            run_id=run_id,
            dataset=result["dataset"],
            stock_id=result["stock_id"],
            query_mode=result["query_mode"],
            start_date=result.get("start_date"),
            end_date=result.get("end_date"),
            requested_params_json=requested_params_json,
            fetched_rows=int(result.get("fetched_rows", 0)),
            inserted_rows=int(result.get("inserted_rows", 0)),
            status="success",
            error_message=None,
        )
        _print_success(result)
        return 0
    except ValueError as exc:
        try_log_meta_run(
            fallback.get("db_path"),
            run_id=run_id,
            dataset=str(fallback.get("dataset", "unknown")),
            stock_id=str(fallback.get("stock_id", "__UNKNOWN__")),
            query_mode=str(fallback.get("query_mode", "unknown")),
            start_date=fallback.get("start_date"),
            end_date=fallback.get("end_date"),
            requested_params_json=requested_params_json,
            fetched_rows=0,
            inserted_rows=0,
            status="error",
            error_message=str(exc),
        )
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except APIError as exc:
        try_log_meta_run(
            fallback.get("db_path"),
            run_id=run_id,
            dataset=str(fallback.get("dataset", "unknown")),
            stock_id=str(fallback.get("stock_id", "__UNKNOWN__")),
            query_mode=str(fallback.get("query_mode", "unknown")),
            start_date=fallback.get("start_date"),
            end_date=fallback.get("end_date"),
            requested_params_json=requested_params_json,
            fetched_rows=0,
            inserted_rows=0,
            status="error",
            error_message=str(exc),
        )
        print(f"Error: {exc}", file=sys.stderr)
        return 3
    except (sqlite3.Error, OSError) as exc:
        try_log_meta_run(
            fallback.get("db_path"),
            run_id=run_id,
            dataset=str(fallback.get("dataset", "unknown")),
            stock_id=str(fallback.get("stock_id", "__UNKNOWN__")),
            query_mode=str(fallback.get("query_mode", "unknown")),
            start_date=fallback.get("start_date"),
            end_date=fallback.get("end_date"),
            requested_params_json=requested_params_json,
            fetched_rows=0,
            inserted_rows=0,
            status="error",
            error_message=str(exc),
        )
        print(f"Error: {exc}", file=sys.stderr)
        return 4
    except Exception as exc:  # pylint: disable=broad-except
        try_log_meta_run(
            fallback.get("db_path"),
            run_id=run_id,
            dataset=str(fallback.get("dataset", "unknown")),
            stock_id=str(fallback.get("stock_id", "__UNKNOWN__")),
            query_mode=str(fallback.get("query_mode", "unknown")),
            start_date=fallback.get("start_date"),
            end_date=fallback.get("end_date"),
            requested_params_json=requested_params_json,
            fetched_rows=0,
            inserted_rows=0,
            status="error",
            error_message=str(exc),
        )
        print(f"Error: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
