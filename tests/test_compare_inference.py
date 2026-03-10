from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.schema import init_schema
from research.compare_inference import main as compare_inference_main
from research.run import main as run_main


def _create_local_research_data(data_root: Path) -> None:
    conn = sqlite3.connect(data_root / "market.sqlite")
    init_schema(conn)
    for stock_id, stock_name in [("2330", "TSMC"), ("2317", "HonHai")]:
        conn.execute(
            "INSERT INTO stock_info (date, stock_id, stock_name, type, industry_category) VALUES (?, ?, ?, ?, ?)",
            ("2024-01-01", stock_id, stock_name, "twse", "Electronics"),
        )
    conn.commit()
    conn.close()

    for stock_id, drift in [("2330", 0.0011), ("2317", 0.0009)]:
        conn = sqlite3.connect(data_root / f"{stock_id}.sqlite")
        init_schema(conn)
        start = pd.Timestamp("2024-01-01")
        rows = []
        close = 100.0 + (1 if stock_id == "2317" else 0)
        for idx in range(260):
            close *= 1.0 + drift + ((idx % 7) - 3) * 0.0002
            date = (start + pd.Timedelta(days=idx)).strftime("%Y-%m-%d")
            rows.append((date, close - 1, close + 1, close - 2, close, 1000 + idx, 100000 + idx, 0.1, 10, 0))
        conn.executemany(
            """
            INSERT INTO price_adj_daily (
                date, open, max, min, close, trading_volume,
                trading_money, spread, trading_turnover, is_placeholder
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        conn.close()


def _write_spec(path: Path) -> None:
    payload = {
        "research_id": "ma_cross_inference_compare_test",
        "title": "MA Cross Inference Compare Test",
        "description": "Inference run comparison test.",
        "pipeline_id": "ma_crossover",
        "required_datasets": ["stock_info", "price_adj"],
        "data_update_policy": {
            "mode": "ensure_local",
            "auto_update_missing": False,
            "auto_update_stale": False,
            "stock_info_start_date": "2024-01-01",
        },
        "analysis_period": {"start_date": "2024-01-01"},
        "universe_definition": {
            "stock_ids": ["2330", "2317"],
            "exclude_etf": True,
            "exclude_warrant": True,
            "min_history_days": 30,
            "inactive_lookback_days": 20,
            "adjusted_price": True,
        },
        "feature_definition": {"ma_windows": [5, 20], "vol_windows": [20]},
        "strategy_definition": {
            "name": "ma_crossover",
            "short_window": 5,
            "long_window": 20,
            "use_adjusted_price": True,
            "use_legacy_impl": False,
        },
        "backtest_definition": {"transaction_cost_bps": 5.0, "slippage_bps": 1.0, "lag_positions": 1},
        "evaluation_definition": {
            "bootstrap_samples": 50,
            "seed": 7,
            "walk_forward_train_window": 30,
            "walk_forward_test_window": 10,
            "walk_forward_step": 10,
            "rolling_window": 10,
            "expanding_min_periods": 10,
        },
        "report_definition": {"write_universe_csv": True, "write_features_csv": False},
        "rerun_mode": "fixed_spec",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class CompareInferenceTests(unittest.TestCase):
    def test_compare_inference_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            experiments_root = root / "experiments"
            data_root.mkdir(parents=True, exist_ok=True)
            _create_local_research_data(data_root)

            spec_path = root / "spec.json"
            _write_spec(spec_path)

            for run_id, as_of in [("run_a", "2024-06-30"), ("run_b", "2024-08-31")]:
                exit_code = run_main(
                    [
                        "--spec",
                        str(spec_path),
                        "--data-as-of",
                        as_of,
                        "--run-id",
                        run_id,
                        "--data-root",
                        str(data_root),
                        "--experiments-root",
                        str(experiments_root),
                        "--catalog-path",
                        str(ROOT / "data" / "catalog" / "data_catalog.yaml"),
                        "--feature-store-version",
                        "itest",
                    ]
                )
                self.assertEqual(exit_code, 0)

            output_json = root / "compare_inference.json"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = compare_inference_main(
                    [
                        "--research-id",
                        "ma_cross_inference_compare_test",
                        "--base-run",
                        "run_a",
                        "--target-run",
                        "run_b",
                        "--experiments-root",
                        str(experiments_root),
                        "--output-json",
                        str(output_json),
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue(output_json.exists())
            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["base_run_id"], "run_a")
            self.assertEqual(payload["target_run_id"], "run_b")
            self.assertIn("coefficient_stability", payload)
            self.assertIn("spread_changes", payload)
            self.assertIn("Coefficient Stability", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()

