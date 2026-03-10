from __future__ import annotations

import sqlite3
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets import price, stock_info


class StorageLayoutTests(unittest.TestCase):
    def test_per_stock_db_identity_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "custom.sqlite"
            args_a = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-01",
                db_path=str(db_path),
                replace=True,
            )
            args_b = Namespace(
                stock_id="2317",
                start_date="2026-03-01",
                end_date="2026-03-01",
                db_path=str(db_path),
                replace=False,
            )

            def fake_fetch(dataset: str, _token: str, _params: dict[str, str]):
                if dataset == "TaiwanStockTradingDate":
                    return [{"date": "2026-03-01"}]
                if dataset == "TaiwanStockPrice":
                    return []
                raise AssertionError(dataset)

            with patch("finmind_dl.datasets.price_like.fetch_dataset", side_effect=fake_fetch), patch(
                "finmind_dl.datasets.common.fetch_dataset", side_effect=fake_fetch
            ):
                price.run(args_a, token="dummy")
                with self.assertRaises(ValueError):
                    price.run(args_b, token="dummy")

            conn = sqlite3.connect(db_path)
            try:
                db_identity = conn.execute(
                    'SELECT stock_id FROM db_identity WHERE id = 1'
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(db_identity[0], "2330")

    def test_legacy_shared_files_auto_migrate_to_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy_stock = root / "stock_info.sqlite"
            legacy_holding = root / "holding_shares_per.sqlite"
            market_db = root / "market.sqlite"

            conn = sqlite3.connect(legacy_stock)
            try:
                conn.execute(
                    """
                    CREATE TABLE stock_info (
                        date TEXT,
                        stock_id TEXT,
                        stock_name TEXT,
                        type TEXT,
                        industry_category TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO stock_info VALUES (?, ?, ?, ?, ?)",
                    ("2026-03-01", "2330", "TSMC", "twse", "Semiconductor"),
                )
                conn.commit()
            finally:
                conn.close()

            conn = sqlite3.connect(legacy_holding)
            try:
                conn.execute(
                    """
                    CREATE TABLE holding_shares_per (
                        date TEXT,
                        stock_id TEXT,
                        holding_shares_level TEXT,
                        people INTEGER,
                        percent REAL,
                        unit INTEGER,
                        query_mode TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO holding_shares_per VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("2026-03-01", "2330", "1-5", 10, 1.2, 1000, "all_market_date"),
                )
                conn.commit()
            finally:
                conn.close()

            args = Namespace(start_date="2026-03-01", db_path=str(market_db), replace=False)
            with patch("finmind_dl.datasets.stock_info.fetch_dataset", return_value=[]):
                result = stock_info.run(args, token="dummy")

            self.assertEqual(Path(result["db_path"]), market_db)
            self.assertFalse(legacy_stock.exists())
            self.assertFalse(legacy_holding.exists())

            conn = sqlite3.connect(market_db)
            try:
                stock_rows = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
                holding_rows = conn.execute("SELECT COUNT(*) FROM holding_shares_per").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(stock_rows, 1)
            self.assertEqual(holding_rows, 1)

    def test_legacy_per_stock_table_auto_migrates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE price_daily (
                        date TEXT NOT NULL,
                        stock_id TEXT NOT NULL,
                        open REAL,
                        max REAL,
                        min REAL,
                        close REAL,
                        trading_volume INTEGER,
                        trading_money INTEGER,
                        spread REAL,
                        trading_turnover INTEGER,
                        is_placeholder INTEGER NOT NULL DEFAULT 0,
                        inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
                        UNIQUE (date, stock_id)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO price_daily (
                        date, stock_id, open, max, min, close,
                        trading_volume, trading_money, spread, trading_turnover, is_placeholder
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-03-01", "2330", 100.0, 110.0, 90.0, 105.0, 1000, 100000, 1.0, 10, 0),
                )
                conn.commit()
            finally:
                conn.close()

            args = Namespace(
                stock_id="2330",
                start_date="2026-03-01",
                end_date="2026-03-01",
                db_path=str(db_path),
                replace=False,
            )

            def fake_fetch(dataset: str, _token: str, _params: dict[str, str]):
                if dataset == "TaiwanStockTradingDate":
                    return [{"date": "2026-03-01"}]
                if dataset == "TaiwanStockPrice":
                    return []
                raise AssertionError(dataset)

            with patch("finmind_dl.datasets.price_like.fetch_dataset", side_effect=fake_fetch), patch(
                "finmind_dl.datasets.common.fetch_dataset", side_effect=fake_fetch
            ):
                price.run(args, token="dummy")

            conn = sqlite3.connect(db_path)
            try:
                cols = {
                    row[1]
                    for row in conn.execute('PRAGMA table_info("price_daily")').fetchall()
                }
                row = conn.execute(
                    "SELECT open, close, is_placeholder FROM price_daily WHERE date = ?",
                    ("2026-03-01",),
                ).fetchone()
                identity = conn.execute(
                    'SELECT stock_id FROM db_identity WHERE id = 1'
                ).fetchone()
            finally:
                conn.close()

            self.assertNotIn("stock_id", cols)
            self.assertEqual(row, (100.0, 105.0, 0))
            self.assertEqual(identity[0], "2330")


if __name__ == "__main__":
    unittest.main()
