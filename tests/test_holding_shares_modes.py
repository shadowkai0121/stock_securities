from __future__ import annotations

import sqlite3
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets import holding_shares


class HoldingSharesModeTests(unittest.TestCase):
    def test_stock_range_and_all_market_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stock_db = Path(tmp) / "2330.sqlite"
            market_db = Path(tmp) / "holding.sqlite"

            stock_args = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-01",
                all_market_date=None,
                db_path=str(stock_db),
                replace=True,
            )
            all_market_args = Namespace(
                stock_id=None,
                start_date=None,
                end_date=None,
                all_market_date="2026-03-01",
                db_path=str(market_db),
                replace=True,
            )

            with patch(
                "finmind_dl.datasets.holding_shares.fetch_dataset",
                return_value=[
                    {
                        "date": "2026-03-01",
                        "stock_id": "2330",
                        "HoldingSharesLevel": "1-5",
                        "people": 10,
                        "percent": 1.5,
                        "unit": 1000,
                    }
                ],
            ):
                holding_shares.run(stock_args, token="dummy")

            conn = sqlite3.connect(stock_db)
            try:
                query_mode = conn.execute(
                    "SELECT query_mode FROM holding_shares_per LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(query_mode, "stock_range")

            with patch(
                "finmind_dl.datasets.holding_shares.fetch_dataset",
                return_value=[
                    {
                        "date": "2026-03-01",
                        "stock_id": "1101",
                        "HoldingSharesLevel": "1-5",
                        "people": 99,
                        "percent": 3.2,
                        "unit": 2000,
                    }
                ],
            ):
                holding_shares.run(all_market_args, token="dummy")

            conn = sqlite3.connect(market_db)
            try:
                query_mode = conn.execute(
                    "SELECT query_mode FROM holding_shares_per LIMIT 1"
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(query_mode, "all_market_date")


if __name__ == "__main__":
    unittest.main()
