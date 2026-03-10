from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from features.feature_store import FeatureStore


class FeatureStoreTests(unittest.TestCase):
    def test_feature_store_build_and_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            cache_dir = Path(tmp_name) / "cache"
            store = FeatureStore(cache_dir=cache_dir, version="vtest")

            dates = pd.date_range("2024-01-01", periods=80, freq="D")
            rows = []
            for stock_id in ["2330", "2317"]:
                base = 100.0
                for idx, dt in enumerate(dates):
                    base += 0.1
                    rows.append(
                        {
                            "date": dt.strftime("%Y-%m-%d"),
                            "stock_id": stock_id,
                            "close": base,
                            "trading_money": 100000 + idx,
                            "trading_volume": 1000 + idx,
                        }
                    )
            price_df = pd.DataFrame(rows)

            key = store.build_cache_key(
                feature_set_name="default",
                universe_definition={"name": "u1"},
                feature_definition={"ma_windows": [5, 20]},
            )

            created = {"count": 0}

            def builder() -> pd.DataFrame:
                created["count"] += 1
                return store.build_features(price_df=price_df, ma_windows=[5, 20], vol_windows=[20])

            frame1 = store.get_or_create(
                key=key,
                builder=builder,
                metadata={"k": "v"},
            )
            frame2 = store.get_or_create(
                key=key,
                builder=builder,
                metadata={"k": "v"},
            )

            self.assertGreater(len(frame1), 0)
            self.assertEqual(created["count"], 1)
            self.assertEqual(len(frame1), len(frame2))
            self.assertTrue((cache_dir / "vtest" / f"{key}.parquet").exists())
            self.assertIn("ma_5", frame1.columns)
            self.assertIn("volatility_20", frame1.columns)


if __name__ == "__main__":
    unittest.main()
