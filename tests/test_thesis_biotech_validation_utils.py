from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from _bootstrap import ROOT  # noqa: F401

PAPERS = ROOT / "papers"
if str(PAPERS) not in sys.path:
    sys.path.insert(0, str(PAPERS))

import thesis_biotech_validation_utils as utils


class BrokerMappingTests(unittest.TestCase):
    def test_resolve_mapping_success(self) -> None:
        available = ["富邦建國", "港商野村", "凱基台北"]
        mapping_cfg = {
            "富邦建國": {"candidate_names": ["富邦建國"]},
            "野村": {"candidate_names": ["野村", "港商野村"]},
        }
        resolved = utils.resolve_broker_mapping(available, mapping_cfg)
        self.assertEqual(resolved["富邦建國"], "富邦建國")
        self.assertEqual(resolved["野村"], "港商野村")

    def test_resolve_mapping_missing_raises(self) -> None:
        with self.assertRaises(ValueError) as cm:
            utils.resolve_broker_mapping(
                ["富邦建國"],
                {"日盛南京": {"candidate_names": ["日盛南京"]}},
            )
        self.assertIn("Missing mapping", str(cm.exception))

    def test_resolve_mapping_ambiguous_raises(self) -> None:
        with self.assertRaises(ValueError) as cm:
            utils.resolve_broker_mapping(
                ["A", "B"],
                {"X": {"candidate_names": ["A", "B"]}},
            )
        self.assertIn("Ambiguous mapping", str(cm.exception))


class PanelBuildTests(unittest.TestCase):
    def test_build_stock_panel_and_filter_no_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE price_adj_daily (
                        date TEXT,
                        stock_id TEXT,
                        close REAL,
                        is_placeholder INTEGER
                    );
                    CREATE TABLE broker_trades (
                        date TEXT,
                        stock_id TEXT,
                        broker_id TEXT,
                        broker_name TEXT,
                        price REAL,
                        buy REAL,
                        sell REAL,
                        is_placeholder INTEGER
                    );
                    """
                )
                conn.executemany(
                    "INSERT INTO price_adj_daily (date, stock_id, close, is_placeholder) VALUES (?, ?, ?, ?)",
                    [
                        ("2020-03-02", "2330", 10.0, 0),
                        ("2020-03-03", "2330", 11.0, 0),
                        ("2020-03-04", "2330", 11.0, 0),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO broker_trades (date, stock_id, broker_id, broker_name, price, buy, sell, is_placeholder)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("2020-03-02", "2330", "A1", "BrokerA", 10.0, 10.0, 2.0, 0),
                        ("2020-03-03", "2330", "A1", "BrokerA", 11.0, 5.0, 1.0, 0),
                        ("2020-03-03", "2330", "B1", "BrokerB", 11.0, 0.0, 3.0, 0),
                        ("2020-03-03", "2330", "__NO_DATA__", "__NO_DATA__", None, None, None, 1),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            panel_df, broker_df = utils.build_stock_panel(
                db_path,
                {"富邦建國": "BrokerA", "凱基台北": "BrokerB"},
            )

            self.assertIn("富邦建國", panel_df.columns)
            self.assertIn("凱基台北", panel_df.columns)
            self.assertEqual(int((broker_df["mapped_broker"] == "__NO_DATA__").sum()), 0)

            day2 = panel_df[panel_df["date"] == pd.Timestamp("2020-03-03")].iloc[0]
            self.assertAlmostEqual(float(day2["富邦建國"]), 44.0, places=6)
            self.assertAlmostEqual(float(day2["凱基台北"]), -33.0, places=6)


class DownloadCommandTests(unittest.TestCase):
    def test_run_cli_download_command_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"

            with patch("thesis_biotech_validation_utils.subprocess.run") as mock_run:
                utils.run_cli_download(
                    stock_id="2330",
                    start_date="2020-03-02",
                    end_date="2020-07-01",
                    db_path=db_path,
                    refetch=True,
                    token="dummy-token",
                    python_executable="python",
                )

                self.assertEqual(mock_run.call_count, 2)
                first_cmd = mock_run.call_args_list[0].args[0]
                second_cmd = mock_run.call_args_list[1].args[0]
                self.assertIn("price-adj", first_cmd)
                self.assertIn("--replace", first_cmd)
                self.assertIn("broker", second_cmd)
                self.assertNotIn("--replace", second_cmd)


class ModelAndSummaryTests(unittest.TestCase):
    def test_model_1_2_and_model_3(self) -> None:
        dates = pd.date_range("2020-03-02", periods=12, freq="D")
        x = np.arange(1.0, 13.0)
        panel_df = pd.DataFrame(
            {
                "date": dates,
                "stock_id": "2330",
                "return": 0.5 * x,
                "富邦建國": x,
                "富邦敦南": -0.2 * x,
                "台新建北": 0.3 * x,
            }
        )

        groups = {
            "famous": ["富邦建國"],
            "unfamous_same": ["富邦敦南"],
            "unfamous_near": ["台新建北"],
        }

        model12 = utils.run_model_1_2_returns(panel_df, groups)
        self.assertEqual(len(model12), 3)
        famous_row = model12[model12["broker_name"] == "富邦建國"].iloc[0]
        self.assertAlmostEqual(float(famous_row["beta"]), 0.5, places=6)

        panel_m3 = panel_df.copy()
        panel_m3["富邦敦南"] = 2.0 * panel_m3["富邦建國"].shift(1).fillna(0.0)
        panel_m3["台新建北"] = -1.0 * panel_m3["富邦建國"].shift(1).fillna(0.0)

        model3 = utils.run_model_3_herding(panel_m3, groups)
        same_row = model3[model3["group"] == "unfamous_same"].iloc[0]
        near_row = model3[model3["group"] == "unfamous_near"].iloc[0]
        self.assertAlmostEqual(float(same_row["beta"]), 2.0, places=6)
        self.assertAlmostEqual(float(near_row["beta"]), -1.0, places=6)

    def test_summary_and_compare_benchmark(self) -> None:
        sample = pd.DataFrame(
            {
                "group": ["famous", "famous", "famous", "famous"],
                "p_value": [0.005, 0.03, 0.07, 0.2],
            }
        )
        summary = utils.summarize_significance(sample)

        benchmark = {
            "famous": {
                "<1%": {"count": 1, "ratio": 0.25},
                "1%~5%": {"count": 1, "ratio": 0.25},
                "5%~10%": {"count": 1, "ratio": 0.25},
            }
        }
        compare = utils.compare_with_benchmark(summary, benchmark)
        self.assertTrue((compare["delta_count"] == 0).all())


if __name__ == "__main__":
    unittest.main()
