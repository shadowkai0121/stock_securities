from __future__ import annotations

import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets import daily


class DailyBundleTests(unittest.TestCase):
    def test_daily_bundle_default_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            args = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-03",
                db_path=str(db_path),
                replace=True,
                include_holding_shares=False,
            )

            calls: list[tuple[str, bool]] = []

            def _mk_handler(name: str):
                def _handler(child_args: Namespace, token: str):
                    calls.append((name, bool(child_args.replace)))
                    return {
                        "dataset": name,
                        "table": f"{name}_table",
                        "stock_id": child_args.stock_id,
                        "query_mode": "stock_range",
                        "start_date": child_args.start_date,
                        "end_date": child_args.end_date,
                        "db_path": Path(child_args.db_path),
                        "fetched_rows": 3,
                        "inserted_rows": 2,
                    }

                return _handler

            with patch("finmind_dl.datasets.daily.price.run", side_effect=_mk_handler("price")), patch(
                "finmind_dl.datasets.daily.price_adj.run", side_effect=_mk_handler("price-adj")
            ), patch("finmind_dl.datasets.daily.margin.run", side_effect=_mk_handler("margin")), patch(
                "finmind_dl.datasets.daily.broker.run", side_effect=_mk_handler("broker")
            ):
                result = daily.run(args, token="dummy")

        self.assertEqual(
            calls,
            [
                ("price", True),
                ("price-adj", False),
                ("margin", False),
                ("broker", False),
            ],
        )
        self.assertEqual(result["dataset"], daily.DATASET)
        self.assertEqual(result["fetched_rows"], 12)
        self.assertEqual(result["inserted_rows"], 8)

    def test_daily_bundle_include_holding_shares(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            args = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-03",
                db_path=str(db_path),
                replace=False,
                include_holding_shares=True,
            )

            called = {"holding": 0}

            def _basic(_args: Namespace, _token: str):
                return {
                    "dataset": "x",
                    "table": "x",
                    "stock_id": "2330",
                    "query_mode": "stock_range",
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-03",
                    "db_path": db_path,
                    "fetched_rows": 1,
                    "inserted_rows": 1,
                }

            def _holding(_args: Namespace, _token: str):
                called["holding"] += 1
                return {
                    "dataset": "holding-shares",
                    "table": "holding_shares_per",
                    "stock_id": "2330",
                    "query_mode": "stock_range",
                    "start_date": "2026-03-01",
                    "end_date": "2026-03-03",
                    "db_path": db_path,
                    "fetched_rows": 1,
                    "inserted_rows": 1,
                }

            with patch("finmind_dl.datasets.daily.price.run", side_effect=_basic), patch(
                "finmind_dl.datasets.daily.price_adj.run", side_effect=_basic
            ), patch("finmind_dl.datasets.daily.margin.run", side_effect=_basic), patch(
                "finmind_dl.datasets.daily.broker.run", side_effect=_basic
            ), patch("finmind_dl.datasets.daily.holding_shares.run", side_effect=_holding):
                result = daily.run(args, token="dummy")

        self.assertEqual(called["holding"], 1)
        self.assertEqual(result["fetched_rows"], 5)


if __name__ == "__main__":
    unittest.main()
