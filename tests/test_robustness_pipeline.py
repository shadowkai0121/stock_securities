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
from research.run import main as run_main


def _create_local_research_data(data_root: Path) -> None:
    conn = sqlite3.connect(data_root / "market.sqlite")
    init_schema(conn)
    conn.execute(
        "INSERT INTO stock_info (date, stock_id, stock_name, type, industry_category) VALUES (?, ?, ?, ?, ?)",
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(data_root / "2330.sqlite")
    init_schema(conn)
    start = pd.Timestamp("2024-01-01")
    rows = []
    close = 100.0
    for idx in range(220):
        close *= 1.001 + ((idx % 7) - 3) * 0.0002
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
        "research_id": "ma_cross_robustness_test",
        "title": "MA Cross Robustness Test",
        "description": "Robustness scenario generation test.",
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
            "stock_ids": ["2330"],
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
        "robustness": {
            "transaction_costs": [0, 20],
            "holding_periods": [1],
            "winsorization_levels": [0],
        },
        "rerun_mode": "fixed_spec",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class RobustnessPipelineTests(unittest.TestCase):
    def test_runner_generates_robustness_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            experiments_root = root / "experiments"
            data_root.mkdir(parents=True, exist_ok=True)
            _create_local_research_data(data_root)

            spec_path = root / "spec.json"
            _write_spec(spec_path)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = run_main(
                    [
                        "--spec",
                        str(spec_path),
                        "--data-as-of",
                        "2024-07-31",
                        "--run-id",
                        "run_robust",
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
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["run_id"], "run_robust")

            run_dir = experiments_root / "ma_cross_robustness_test" / "runs" / "run_robust"
            robustness_json = run_dir / "robustness" / "robustness_results.json"
            self.assertTrue(robustness_json.exists())

            robustness_payload = json.loads(robustness_json.read_text(encoding="utf-8"))
            self.assertEqual(len(robustness_payload["scenarios"]), 2)
            self.assertEqual(len(robustness_payload["errors"]), 0)

            metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
            self.assertIn("robustness_summary", metrics)
            self.assertEqual(metrics["robustness_summary"]["succeeded"], 2)


if __name__ == "__main__":
    unittest.main()

