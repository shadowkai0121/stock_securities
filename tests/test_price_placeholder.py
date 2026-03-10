from __future__ import annotations

import sqlite3
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets import price


class PricePlaceholderTests(unittest.TestCase):
    def test_placeholder_then_real_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"

            args = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-01",
                db_path=str(db_path),
                replace=True,
            )

            def first_fetch(dataset: str, token: str, params: dict[str, str]):
                if dataset == "TaiwanStockTradingDate":
                    return [{"date": "2026-03-01"}]
                if dataset == "TaiwanStockPrice":
                    return []
                raise AssertionError(dataset)

            with patch("finmind_dl.datasets.price_like.fetch_dataset", side_effect=first_fetch), patch(
                "finmind_dl.datasets.common.fetch_dataset", side_effect=first_fetch
            ):
                price.run(args, token="dummy")

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT is_placeholder, open FROM price_daily WHERE date = '2026-03-01'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], 1)
            self.assertIsNone(row[1])

            args.replace = False

            def second_fetch(dataset: str, token: str, params: dict[str, str]):
                if dataset == "TaiwanStockTradingDate":
                    return [{"date": "2026-03-01"}]
                if dataset == "TaiwanStockPrice":
                    return [
                        {
                            "date": "2026-03-01",
                            "stock_id": "2330",
                            "open": 100,
                            "max": 110,
                            "min": 90,
                            "close": 105,
                            "Trading_Volume": 200,
                            "Trading_money": 300,
                            "spread": 1,
                            "Trading_turnover": 4,
                        }
                    ]
                raise AssertionError(dataset)

            with patch("finmind_dl.datasets.price_like.fetch_dataset", side_effect=second_fetch), patch(
                "finmind_dl.datasets.common.fetch_dataset", side_effect=second_fetch
            ):
                price.run(args, token="dummy")

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT is_placeholder, open, close FROM price_daily WHERE date = '2026-03-01'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], 0)
            self.assertEqual(row[1], 100.0)
            self.assertEqual(row[2], 105.0)


if __name__ == "__main__":
    unittest.main()
