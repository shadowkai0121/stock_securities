from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from experiments.registry import ResearchRunRegistry
from research.specs import SpecValidationError, load_research_spec, resolve_research_spec


def _write_spec(path: Path) -> None:
    payload = {
        "research_id": "ma_cross_example_v1",
        "title": "MA Cross Example",
        "description": "Stable MA crossover rerun spec.",
        "pipeline_id": "ma_crossover",
        "required_datasets": ["stock_info", "price_adj"],
        "data_update_policy": {
            "mode": "ensure_local",
            "auto_update_missing": True,
            "auto_update_stale": True,
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
            "bootstrap_samples": 100,
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


class ResearchSpecAndRegistryTests(unittest.TestCase):
    def test_spec_parsing_and_resolution_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            spec_path = Path(tmp_name) / "spec.json"
            _write_spec(spec_path)

            spec = load_research_spec(spec_path)
            resolved = resolve_research_spec(
                spec,
                data_as_of="2024-08-31",
                run_id="run_001",
                data_root="data",
                experiments_root="experiments",
                catalog_path="data/catalog/data_catalog.yaml",
                feature_store_version="itest",
                git_commit_hash="abc123",
            )

            self.assertEqual(spec.research_id, "ma_cross_example_v1")
            self.assertEqual(resolved["data_as_of"], "2024-08-31")
            self.assertEqual(resolved["analysis_period"]["start_date"], "2024-01-01")
            self.assertEqual(resolved["analysis_period"]["data_as_of"], "2024-08-31")
            self.assertEqual(resolved["runtime"]["feature_store_version"], "itest")
            self.assertEqual(resolved["code_version"]["git_commit_hash"], "abc123")
            self.assertTrue(resolved["spec_sha256"])

    def test_invalid_rerun_mode_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            spec_path = Path(tmp_name) / "bad_spec.json"
            _write_spec(spec_path)
            payload = json.loads(spec_path.read_text(encoding="utf-8"))
            payload["rerun_mode"] = "retrain_parameters"
            spec_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with self.assertRaises(SpecValidationError):
                load_research_spec(spec_path)

    def test_run_registry_is_append_only_and_updates_latest_pointer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            registry = ResearchRunRegistry(root_dir=Path(tmp_name) / "experiments")

            run_a = registry.create_run(
                research_id="ma_cross_example_v1",
                data_as_of="2024-06-30",
                spec_path="research_specs/ma_cross_example_v1.json",
                rerun_mode="fixed_spec",
                run_id="run_a",
            )
            registry.write_run_snapshot(
                record=run_a,
                resolved_spec={"research_id": "ma_cross_example_v1", "run_id": "run_a", "data_as_of": "2024-06-30"},
                data_manifest={"dataset_fingerprint": "fp_a"},
            )
            registry.mark_succeeded(
                record=run_a,
                metrics={"annual_return": 0.1, "sharpe_ratio": 1.2, "universe_size": 1},
                artifacts={"report": "report.md"},
                data_manifest={"dataset_fingerprint": "fp_a"},
            )

            run_b = registry.create_run(
                research_id="ma_cross_example_v1",
                data_as_of="2024-08-31",
                spec_path="research_specs/ma_cross_example_v1.json",
                rerun_mode="fixed_spec",
                run_id="run_b",
            )
            registry.write_run_snapshot(
                record=run_b,
                resolved_spec={"research_id": "ma_cross_example_v1", "run_id": "run_b", "data_as_of": "2024-08-31"},
                data_manifest={"dataset_fingerprint": "fp_b"},
            )
            registry.mark_succeeded(
                record=run_b,
                metrics={"annual_return": 0.2, "sharpe_ratio": 1.4, "universe_size": 1},
                artifacts={"report": "report.md"},
                data_manifest={"dataset_fingerprint": "fp_b"},
            )

            runs = registry.list_runs("ma_cross_example_v1")
            latest = registry.latest_run("ma_cross_example_v1")
            previous = registry.previous_run("ma_cross_example_v1")

            self.assertEqual(len(runs), 2)
            self.assertEqual(latest["run_id"], "run_b")
            self.assertEqual(previous["run_id"], "run_a")
            self.assertTrue((run_a.path / "resolved_spec.json").exists())
            self.assertTrue((run_b.path / "metrics.json").exists())

            with self.assertRaises(FileExistsError):
                registry.create_run(
                    research_id="ma_cross_example_v1",
                    data_as_of="2024-08-31",
                    spec_path="research_specs/ma_cross_example_v1.json",
                    rerun_mode="fixed_spec",
                    run_id="run_b",
                )


if __name__ == "__main__":
    unittest.main()
