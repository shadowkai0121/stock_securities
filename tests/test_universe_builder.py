from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from research.data_loader import ResearchDataLoader
from universe.universe_builder import TaiwanEquityUniverseBuilder


def _write_stock_info(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
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
    rows = [
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
        ("2024-01-01", "0050", "Taiwan 50 ETF", "twse", "ETF"),
        ("2024-01-01", "1101", "CementCo", "twse", "Cement"),
    ]
    conn.executemany("INSERT INTO stock_info VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_price(db_path: Path, stock_id: str, days: int) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE price_adj_daily (
            date TEXT,
            stock_id TEXT,
            open REAL,
            max REAL,
            min REAL,
            close REAL,
            trading_volume INTEGER,
            trading_money INTEGER,
            spread REAL,
            trading_turnover INTEGER,
            is_placeholder INTEGER
        )
        """
    )
    start = pd.Timestamp("2024-01-01")
    rows = []
    price = 100.0
    for idx in range(days):
        date = (start + pd.Timedelta(days=idx)).strftime("%Y-%m-%d")
        price += 0.5
        rows.append((date, stock_id, price - 1, price + 1, price - 2, price, 1000 + idx, 100000 + idx, 0.1, 5, 0))
    conn.executemany(
        """
        INSERT INTO price_adj_daily (
            date, stock_id, open, max, min, close,
            trading_volume, trading_money, spread, trading_turnover, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


class UniverseBuilderTests(unittest.TestCase):
    def test_universe_filters_etf_and_short_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            _write_stock_info(root / "stock_info.sqlite")
            _write_price(root / "2330.sqlite", "2330", 90)
            _write_price(root / "0050.sqlite", "0050", 90)
            _write_price(root / "1101.sqlite", "1101", 20)

            loader = ResearchDataLoader(data_root=root)
            builder = TaiwanEquityUniverseBuilder(loader)

            universe = builder.build(
                start_date="2024-01-01",
                end_date="2024-03-31",
                stock_ids=["2330", "0050", "1101"],
                exclude_etf=True,
                min_history_days=30,
                inactive_lookback_days=30,
                adjusted_price=True,
            )

            self.assertFalse(universe.empty)
            stock_ids = set(universe["stock_id"].unique().tolist())
            self.assertIn("2330", stock_ids)
            self.assertNotIn("0050", stock_ids)
            self.assertNotIn("1101", stock_ids)


if __name__ == "__main__":
    unittest.main()
