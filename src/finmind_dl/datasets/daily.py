"""Bundle handler for one-shot daily downloads by stock id."""

from __future__ import annotations

from argparse import Namespace

from finmind_dl.core.date_utils import ensure_date_range, parse_iso_date

from . import broker, holding_shares, margin, price, price_adj
from .common import default_stock_db_path, summarize_result

DATASET = "daily_bundle"
TABLE = "bundle"


def run(args: Namespace, token: str) -> dict[str, object]:
    start_dt = parse_iso_date(args.start_date, "--start-date")
    end_dt = parse_iso_date(args.end_date, "--end-date")
    ensure_date_range(start_dt, end_dt, start_name="--start-date", end_name="--end-date")

    stock_id = args.stock_id
    db_path = default_stock_db_path(stock_id, args.db_path)

    jobs = [
        ("price", price.run),
        ("price-adj", price_adj.run),
        ("margin", margin.run),
        ("broker", broker.run),
    ]
    if bool(getattr(args, "include_holding_shares", False)):
        jobs.append(("holding-shares", holding_shares.run))

    fetched_total = 0
    inserted_total = 0
    table_names: list[str] = []
    extra_lines: list[str] = []

    for idx, (name, handler) in enumerate(jobs):
        child_args = Namespace(
            stock_id=stock_id,
            start_date=args.start_date,
            end_date=args.end_date,
            db_path=str(db_path),
            replace=bool(args.replace) if idx == 0 else False,
            all_market_date=None,
        )
        result = handler(child_args, token)
        fetched_total += int(result.get("fetched_rows", 0))
        inserted_total += int(result.get("inserted_rows", 0))
        table_name = str(result.get("table", ""))
        if table_name:
            table_names.append(table_name)
        extra_lines.append(
            f"[{name}] fetched={result.get('fetched_rows', 0)} inserted={result.get('inserted_rows', 0)}"
        )

    return summarize_result(
        dataset=DATASET,
        table=", ".join(sorted(set(table_names))),
        stock_id=stock_id,
        query_mode="stock_range_bundle",
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=db_path,
        fetched_rows=fetched_total,
        inserted_rows=inserted_total,
        extra_lines=extra_lines,
        extra={"job_count": len(jobs)},
    )
