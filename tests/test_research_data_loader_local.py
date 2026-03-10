from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from research.data_loader import ResearchDataLoader


def _create_price_db(db_path: Path, stock_id: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE price_adj_daily (
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
            is_placeholder INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE margin_daily (
            date TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            margin_purchase_today_balance INTEGER,
            short_sale_today_balance INTEGER,
            margin_purchase_buy INTEGER,
            margin_purchase_sell INTEGER,
            short_sale_buy INTEGER,
            short_sale_sell INTEGER,
            note TEXT,
            is_placeholder INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE broker_trades (
            date TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            broker_id TEXT NOT NULL,
            buy REAL,
            sell REAL,
            is_placeholder INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE holding_shares_per (
            date TEXT,
            holding_shares_level TEXT,
            people INTEGER,
            percent REAL,
            unit INTEGER,
            query_mode TEXT
        )
        """
    )

    rows_price = [
        ("2024-01-02", stock_id, 100.0, 101.0, 99.0, 100.0, 1000, 100000, 0.1, 10, 0),
        ("2024-01-03", stock_id, 101.0, 102.0, 100.0, 102.0, 1200, 120000, 0.2, 12, 0),
    ]
    conn.executemany(
        """
        INSERT INTO price_adj_daily (
            date, stock_id, open, max, min, close, trading_volume, trading_money,
            spread, trading_turnover, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows_price,
    )

    conn.execute(
        """
        INSERT INTO margin_daily (
            date, stock_id, margin_purchase_today_balance, short_sale_today_balance,
            margin_purchase_buy, margin_purchase_sell, short_sale_buy, short_sale_sell,
            note, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("2024-01-03", stock_id, 1000, 200, 50, 30, 10, 5, "ok", 0),
    )
    conn.execute(
        """
        INSERT INTO broker_trades (
            date, stock_id, broker_id, buy, sell, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("2024-01-03", stock_id, "A001", 500.0, 200.0, 0),
    )
    conn.execute(
        "INSERT INTO holding_shares_per VALUES (?, ?, ?, ?, ?, ?)",
        ("2024-01-03", "1-5", 100, 10.5, 1000, "stock_range"),
    )
    conn.commit()
    conn.close()


def _create_shared_dbs(root: Path) -> None:
    stock_info_db = root / "market.sqlite"
    conn = sqlite3.connect(stock_info_db)
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
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
    )
    conn.commit()
    conn.close()


class ResearchDataLoaderTests(unittest.TestCase):
    def test_local_data_loader_reads_sqlite_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            _create_price_db(root / "2330.sqlite", "2330")
            _create_shared_dbs(root)

            loader = ResearchDataLoader(data_root=root)

            prices = loader.load_prices(
                stock_ids=["2330"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                adjusted=True,
            )
            self.assertEqual(len(prices), 2)

            returns = loader.load_returns(
                stock_ids=["2330"],
                start_date="2024-01-01",
                end_date="2024-01-31",
                adjusted=True,
            )
            self.assertEqual(len(returns), 1)

            margin = loader.load_margin(stock_ids=["2330"], start_date="2024-01-01", end_date="2024-01-31")
            self.assertEqual(len(margin), 1)

            broker = loader.load_broker_flows(stock_ids=["2330"], start_date="2024-01-01", end_date="2024-01-31")
            self.assertEqual(len(broker), 1)
            self.assertIn("imbalance", broker.columns)

            stock_info = loader.load_stock_info(start_date="2024-01-01", end_date="2024-12-31")
            self.assertEqual(len(stock_info), 1)

            holding = loader.load_holding_shares(stock_ids=["2330"], start_date="2024-01-01", end_date="2024-12-31")
            self.assertEqual(len(holding), 1)


if __name__ == "__main__":
    unittest.main()
