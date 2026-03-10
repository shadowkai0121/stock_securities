from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from features.feature_store import FeatureStore
from research.backtest_engine import BacktestConfig, LongCashBacktestEngine
from research.data_loader import ResearchDataLoader
from research.strategies.ma_cross_adapter import MACrossoverSignalModel
from universe.universe_builder import TaiwanEquityUniverseBuilder


def _create_local_dataset(root: Path) -> None:
    conn = sqlite3.connect(root / "stock_info.sqlite")
    conn.execute(
        "CREATE TABLE stock_info (date TEXT, stock_id TEXT, stock_name TEXT, type TEXT, industry_category TEXT)"
    )
    conn.execute(
        "INSERT INTO stock_info VALUES (?, ?, ?, ?, ?)",
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(root / "2330.sqlite")
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
    close = 100.0
    for idx in range(120):
        close *= 1.001 + ((idx % 5) - 2) * 0.0002
        date = (start + pd.Timedelta(days=idx)).strftime("%Y-%m-%d")
        rows.append((date, "2330", close - 1, close + 1, close - 2, close, 1000 + idx, 100000 + idx, 0.1, 10, 0))
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


class PipelineIntegrationTests(unittest.TestCase):
    def test_minimal_local_pipeline_components(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            _create_local_dataset(root)

            loader = ResearchDataLoader(data_root=root)
            universe_builder = TaiwanEquityUniverseBuilder(loader)
            universe = universe_builder.build(
                start_date="2024-01-01",
                end_date="2024-04-30",
                stock_ids=["2330"],
                min_history_days=30,
                inactive_lookback_days=20,
                adjusted_price=True,
            )
            self.assertFalse(universe.empty)

            prices = loader.load_prices(
                stock_ids=["2330"],
                start_date="2024-01-01",
                end_date="2024-04-30",
                adjusted=True,
            )
            self.assertFalse(prices.empty)

            feature_store = FeatureStore(cache_dir=root / "feature_cache", version="itest")
            features = feature_store.build_features(price_df=prices, ma_windows=[5, 20], vol_windows=[20])
            self.assertIn("ma_5", features.columns)

            signal_model = MACrossoverSignalModel(short_window=5, long_window=20, use_legacy_impl=False)
            signals = signal_model.generate_signals(price_df=prices, features=features, universe=universe)
            self.assertFalse(signals.empty)

            engine = LongCashBacktestEngine(BacktestConfig(transaction_cost_bps=5.0, slippage_bps=1.0))
            result = engine.run(
                price_df=prices[["date", "stock_id", "close"]],
                signal_df=signals,
            )

            self.assertIn("annual_return", result.metrics)
            self.assertGreater(len(result.timeseries), 0)


if __name__ == "__main__":
    unittest.main()
