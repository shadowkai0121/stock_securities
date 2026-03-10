from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from research.orchestrator import ResearchOrchestrator


def _create_stock_info(db_path: Path) -> None:
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
    conn.execute(
        "INSERT INTO stock_info VALUES (?, ?, ?, ?, ?)",
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
    )
    conn.commit()
    conn.close()


def _create_price_adj(db_path: Path, stock_id: str) -> None:
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
    for idx in range(220):
        price *= 1.001
        date = (start + pd.Timedelta(days=idx)).strftime("%Y-%m-%d")
        rows.append((date, stock_id, price - 1.0, price + 1.0, price - 2.0, price, 1000 + idx, 100000 + idx, 0.1, 5, 0))

    conn.executemany(
        """
        INSERT INTO price_adj_daily (
            date, stock_id, open, max, min, close, trading_volume,
            trading_money, spread, trading_turnover, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


class OrchestratorSmokeTests(unittest.TestCase):
    def test_orchestrator_runs_end_to_end_on_local_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            experiments_root = root / "experiments"
            data_root.mkdir(parents=True, exist_ok=True)

            _create_stock_info(data_root / "market.sqlite")
            _create_price_adj(data_root / "2330.sqlite", "2330")

            cfg = {
                "experiment_id": "orchestrator_smoke",
                "start_date": "2024-01-01",
                "end_date": "2024-08-31",
                "data_root": str(data_root),
                "experiments_root": str(experiments_root),
                "stock_ids": ["2330"],
                "required_datasets": ["stock_info", "price_adj"],
                "ingestion": {"auto_ingest_missing": False},
                "universe": {
                    "exclude_etf": True,
                    "exclude_warrant": True,
                    "min_history_days": 30,
                    "inactive_lookback_days": 30,
                    "adjusted_price": True,
                },
                "features": {
                    "ma_windows": [5, 20],
                    "vol_windows": [20],
                    "use_margin": False,
                    "use_broker": False,
                    "use_holding_shares": False,
                },
                "strategy": {
                    "name": "ma_crossover",
                    "short_window": 5,
                    "long_window": 20,
                    "use_adjusted_price": True,
                    "use_legacy_impl": False,
                },
                "backtest": {
                    "transaction_cost_bps": 5.0,
                    "slippage_bps": 1.0,
                    "lag_positions": 1,
                },
                "statistics": {
                    "bootstrap_samples": 100,
                    "seed": 7,
                    "walk_forward_train_window": 30,
                    "walk_forward_test_window": 10,
                    "walk_forward_step": 10,
                    "rolling_window": 10,
                    "expanding_min_periods": 10,
                },
            }

            orchestrator = ResearchOrchestrator()
            result = orchestrator.run(cfg)

            self.assertEqual(result["experiment_id"], "orchestrator_smoke")
            self.assertTrue((experiments_root / "orchestrator_smoke" / "metrics.json").exists())
            self.assertTrue((experiments_root / "orchestrator_smoke" / "artifacts.json").exists())
            self.assertTrue((experiments_root / "orchestrator_smoke" / "report.md").exists())
            self.assertIn("annual_return", result["metrics"])


if __name__ == "__main__":
    unittest.main()
