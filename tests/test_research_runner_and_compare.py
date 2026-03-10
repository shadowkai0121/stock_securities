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
from research.compare_runs import main as compare_main
from research.run import main as run_main


def _create_local_research_data(data_root: Path) -> None:
    conn = sqlite3.connect(data_root / "stock_info.sqlite")
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
    for idx in range(260):
        close *= 1.001 + ((idx % 7) - 3) * 0.00015
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


def _write_spec(path: Path) -> None:
    payload = {
        "research_id": "ma_cross_example_v1",
        "title": "MA Cross Example",
        "description": "Stable MA crossover rerun spec.",
        "pipeline_id": "ma_crossover",
        "required_datasets": ["stock_info", "price_adj"],
        "data_update_policy": {
            "mode": "ensure_local",
            "auto_update_missing": False,
            "auto_update_stale": False,
            "stock_info_start_date": "2024-01-01",
        },
        "analysis_period": {
            "start_date": "2024-01-01",
        },
        "universe_definition": {
            "stock_ids": ["2330"],
            "exclude_etf": True,
            "exclude_warrant": True,
            "min_history_days": 30,
            "inactive_lookback_days": 20,
            "adjusted_price": True,
        },
        "feature_definition": {
            "ma_windows": [5, 20],
            "vol_windows": [20],
            "use_margin": False,
            "use_broker": False,
            "use_holding_shares": False,
        },
        "strategy_definition": {
            "name": "ma_crossover",
            "short_window": 5,
            "long_window": 20,
            "use_adjusted_price": True,
            "use_legacy_impl": False,
        },
        "backtest_definition": {
            "transaction_cost_bps": 5.0,
            "slippage_bps": 1.0,
            "lag_positions": 1,
        },
        "evaluation_definition": {
            "bootstrap_samples": 50,
            "seed": 7,
            "walk_forward_train_window": 30,
            "walk_forward_test_window": 10,
            "walk_forward_step": 10,
            "rolling_window": 10,
            "expanding_min_periods": 10,
        },
        "report_definition": {
            "write_timeseries_csv": True,
            "write_universe_csv": True,
        },
        "rerun_mode": "fixed_spec",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class ResearchRunnerAndCompareTests(unittest.TestCase):
    def test_runner_writes_run_artifacts_and_enforces_data_as_of(self) -> None:
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
                        "2024-08-31",
                        "--run-id",
                        "run_a",
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
            self.assertEqual(payload["run_id"], "run_a")

            run_dir = experiments_root / "ma_cross_example_v1" / "runs" / "run_a"
            self.assertTrue((run_dir / "resolved_spec.json").exists())
            self.assertTrue((run_dir / "data_manifest.json").exists())
            self.assertTrue((run_dir / "metrics.json").exists())
            self.assertTrue((run_dir / "artifacts.json").exists())
            self.assertTrue((run_dir / "report.md").exists())
            self.assertTrue((run_dir / "run.log").exists())
            self.assertTrue((run_dir / "plots").exists())

            resolved = json.loads((run_dir / "resolved_spec.json").read_text(encoding="utf-8"))
            manifest = json.loads((run_dir / "data_manifest.json").read_text(encoding="utf-8"))
            metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
            timeseries = pd.read_csv(run_dir / "backtest_timeseries.csv")

            self.assertEqual(resolved["data_as_of"], "2024-08-31")
            self.assertEqual(manifest["data_as_of"], "2024-08-31")
            self.assertIn("annual_return", metrics)
            self.assertIn("statistics", metrics)
            self.assertLessEqual(str(timeseries["date"].max()), "2024-08-31")

    def test_compare_runs_generates_markdown_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            experiments_root = root / "experiments"
            data_root.mkdir(parents=True, exist_ok=True)
            _create_local_research_data(data_root)

            spec_path = root / "spec.json"
            _write_spec(spec_path)

            for run_id, data_as_of in [("run_a", "2024-06-30"), ("run_b", "2024-08-31")]:
                exit_code = run_main(
                    [
                        "--spec",
                        str(spec_path),
                        "--data-as-of",
                        data_as_of,
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

            output_json = root / "compare.json"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = compare_main(
                    [
                        "--research-id",
                        "ma_cross_example_v1",
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
            compare_payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(compare_payload["base_run_id"], "run_a")
            self.assertEqual(compare_payload["target_run_id"], "run_b")
            self.assertIn("annual_return", stdout.getvalue())
            self.assertTrue(compare_payload["datasets"])


if __name__ == "__main__":
    unittest.main()
