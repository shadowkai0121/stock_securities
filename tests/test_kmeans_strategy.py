from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from _bootstrap import ROOT  # noqa: F401


MODULE_PATH = ROOT / "strategies" / "k-means" / "kmeans_clustering.py"
SPEC = importlib.util.spec_from_file_location("kmeans_clustering", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Unable to load strategies/k-means/kmeans_clustering.py")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def create_price_db(db_path: Path, stock_id: str, returns: np.ndarray) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE price_adj_daily (
            date TEXT NOT NULL,
            stock_id TEXT NOT NULL,
            close REAL,
            is_placeholder INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    close = 100.0
    start_date = pd.Timestamp("2024-01-02")
    rows: list[tuple[str, str, float, int]] = []
    for offset, ret in enumerate(returns, start=1):
        close *= float(np.exp(ret))
        rows.append(
            (
                (start_date + pd.Timedelta(days=offset)).strftime("%Y-%m-%d"),
                stock_id,
                close,
                0,
            )
        )
    conn.executemany(
        "INSERT INTO price_adj_daily (date, stock_id, close, is_placeholder) VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


class KMeansStrategyTests(unittest.TestCase):
    def test_run_analysis_builds_expected_outputs(self) -> None:
        rng = np.random.default_rng(7)
        cluster_specs = {
            "1101": (0.0009, 0.0030),
            "1102": (0.0008, 0.0032),
            "1216": (0.0002, 0.0075),
            "1301": (0.0001, 0.0072),
            "2308": (-0.0006, 0.0160),
            "2317": (-0.0007, 0.0155),
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            data_dir = tmp_dir / "data"
            output_dir = tmp_dir / "outputs"
            report_path = tmp_dir / "report.md"
            data_dir.mkdir()

            for stock_id, (mean_return, volatility) in cluster_specs.items():
                returns = rng.normal(loc=mean_return, scale=volatility, size=180)
                create_price_db(data_dir / f"{stock_id}.sqlite", stock_id, returns)

            config = MODULE.ClusteringConfig(
                data_dir=data_dir,
                output_dir=output_dir,
                report_path=report_path,
                min_observations=120,
                random_state=42,
            )
            result = MODULE.run_analysis(config)

            self.assertEqual(result["status"], "success")
            self.assertEqual(result["stocks_clustered"], 6)
            self.assertGreaterEqual(result["best_k"], 2)
            self.assertEqual(
                result["assignments"]["cluster"].nunique(),
                result["best_k"],
            )
            self.assertTrue((output_dir / "feature_matrix.csv").exists())
            self.assertTrue((output_dir / "silhouette_scores.csv").exists())
            self.assertTrue((output_dir / "cluster_assignments.csv").exists())
            self.assertTrue((output_dir / "cluster_summary.csv").exists())
            self.assertTrue((output_dir / "run_summary.json").exists())
            self.assertTrue((output_dir / "kmeans_risk_return_scatter.png").exists())
            self.assertTrue(report_path.exists())

    def test_run_analysis_handles_empty_price_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            data_dir = tmp_dir / "data"
            output_dir = tmp_dir / "outputs"
            report_path = tmp_dir / "report.md"
            data_dir.mkdir()

            conn = sqlite3.connect(data_dir / "2330.sqlite")
            conn.execute(
                """
                CREATE TABLE price_adj_daily (
                    date TEXT NOT NULL,
                    stock_id TEXT NOT NULL,
                    close REAL,
                    is_placeholder INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.commit()
            conn.close()

            config = MODULE.ClusteringConfig(
                data_dir=data_dir,
                output_dir=output_dir,
                report_path=report_path,
                min_observations=5,
            )
            result = MODULE.run_analysis(config)

            self.assertEqual(result["status"], "pending_data")
            self.assertIsNone(result["best_k"])
            self.assertTrue((output_dir / "run_summary.json").exists())
            self.assertTrue(report_path.exists())


if __name__ == "__main__":
    unittest.main()
