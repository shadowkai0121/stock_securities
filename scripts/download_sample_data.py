"""Download a minimal local dataset snapshot through the official ingestion wrapper."""

from __future__ import annotations

import argparse
from pathlib import Path

from data.loaders.finmind_loader import FinMindLoader


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="download-sample-data",
        description="Download a minimal sample dataset via finmind-dl wrapper.",
    )
    parser.add_argument("--data-root", default="data", help="Local data root directory.")
    parser.add_argument("--stock-ids", nargs="+", default=["2330"], help="Stock IDs to download.")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--token", default=None, help="Optional FinMind token override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    data_root = Path(args.data_root)
    data_root.mkdir(parents=True, exist_ok=True)

    loader = FinMindLoader(token=args.token)
    stock_info_result = loader.download_stock_info(
        start_date=args.start_date,
        db_path=data_root / "stock_info.sqlite",
    )
    print(f"[stock_info] inserted={stock_info_result.inserted_rows} db={stock_info_result.db_path}")

    for stock_id in args.stock_ids:
        db_path = data_root / f"{stock_id}.sqlite"
        result = loader.download_price_adj(
            stock_id=stock_id,
            start_date=args.start_date,
            end_date=args.end_date,
            db_path=db_path,
        )
        print(f"[price_adj:{stock_id}] inserted={result.inserted_rows} db={result.db_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
