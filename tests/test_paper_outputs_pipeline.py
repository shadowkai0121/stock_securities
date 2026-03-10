from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.schema import init_schema
from research.paper_outputs.generate import generate_paper_outputs
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

    for stock_id, drift in [("2330", 0.0012), ("2317", 0.0008)]:
        conn = sqlite3.connect(data_root / f"{stock_id}.sqlite")
        init_schema(conn)
        start = pd.Timestamp("2024-01-01")
        rows = []
        close = 100.0 + (2 if stock_id == "2317" else 0)
        for idx in range(260):
            close *= drift + 1.0 + ((idx % 7) - 3) * 0.00015
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
        "research_id": "ma_cross_paper_pipeline_test",
        "title": "MA Cross Paper Pipeline Test",
        "description": "End-to-end experiment-to-paper generation test.",
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


class PaperOutputsPipelineTests(unittest.TestCase):
    def test_experiment_to_paper_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            experiments_root = root / "experiments"
            papers_root = root / "papers"
            data_root.mkdir(parents=True, exist_ok=True)
            _create_local_research_data(data_root)

            spec_path = root / "spec.json"
            _write_spec(spec_path)

            exit_code = run_main(
                [
                    "--spec",
                    str(spec_path),
                    "--data-as-of",
                    "2024-08-31",
                    "--run-id",
                    "run_paper",
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

            manifest = generate_paper_outputs(
                experiment="run_paper",
                paper_id="paper_unit_test",
                research_id="ma_cross_paper_pipeline_test",
                experiments_root=experiments_root,
                papers_root=papers_root,
            )

            paper_root = papers_root / "paper_unit_test"
            self.assertTrue((paper_root / "tables" / "table1_summary_stats.csv").exists())
            self.assertTrue((paper_root / "tables" / "table_main_results.csv").exists())
            self.assertTrue((paper_root / "tables" / "table_portfolio_sort.csv").exists())
            self.assertTrue((paper_root / "figures" / "figure_cumulative_returns.png").exists())
            self.assertTrue((paper_root / "figures" / "figure_coefficients.png").exists())
            self.assertTrue((paper_root / "figures" / "figure_portfolio_spread.png").exists())
            self.assertTrue((paper_root / "figures" / "figure_rolling_performance.png").exists())
            self.assertTrue((paper_root / "appendix" / "appendix_event_study.csv").exists())
            self.assertTrue((paper_root / "appendix" / "appendix_robustness.csv").exists())
            self.assertTrue((paper_root / "appendix" / "appendix_event_study_caar.png").exists())
            self.assertTrue((paper_root / "reproducibility" / "research_spec.json").exists())
            self.assertTrue((paper_root / "reproducibility" / "experiment_run_ids.txt").exists())
            self.assertTrue((paper_root / "reproducibility" / "data_manifest.json").exists())
            self.assertTrue((paper_root / "reproducibility" / "environment_info.json").exists())

            for filename in [
                "abstract.md",
                "introduction.md",
                "literature_review.md",
                "empirical_design.md",
                "results.md",
                "conclusion.md",
            ]:
                self.assertTrue((paper_root / "manuscript" / filename).exists())

            run_dir = experiments_root / "ma_cross_paper_pipeline_test" / "runs" / "run_paper"
            self.assertTrue((run_dir / "inference_results.json").exists())
            self.assertTrue(Path(manifest["manifest_path"]).exists())


if __name__ == "__main__":
    unittest.main()
