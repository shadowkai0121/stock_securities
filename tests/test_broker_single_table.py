from __future__ import annotations

import sqlite3
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets import broker


class BrokerSingleTableTests(unittest.TestCase):
    def test_single_table_insert_and_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "8271.sqlite"
            args = Namespace(
                stock_id="8271",
                start_date="2026-03-01",
                end_date="2026-03-02",
                db_path=str(db_path),
                replace=True,
            )

            with patch(
                "finmind_dl.datasets.broker.fetch_trading_dates",
                return_value=["2026-03-01", "2026-03-02"],
            ), patch(
                "finmind_dl.datasets.broker.fetch_trading_daily_report",
                side_effect=[
                    [
                        {
                            "date": "2026-03-01",
                            "stock_id": "8271",
                            "securities_trader_id": "1160",
                            "securities_trader": "A",
                            "price": "12.3",
                            "buy": "100",
                            "sell": "50",
                        }
                    ],
                    [],
                ],
            ):
                broker.run(args, token="dummy")

            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute("SELECT COUNT(*) FROM broker_trades").fetchone()[0]
                placeholder = conn.execute(
                    "SELECT COUNT(*) FROM broker_trades WHERE broker_id='__NO_DATA__' AND is_placeholder=1"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(count, 2)
            self.assertEqual(placeholder, 1)


if __name__ == "__main__":
    unittest.main()
