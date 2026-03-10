from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from experiments.registry import ExperimentRegistry


class ExperimentRegistryTests(unittest.TestCase):
    def test_registry_creates_and_finalizes_experiment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name) / "experiments"
            registry = ExperimentRegistry(root_dir=root)

            record = registry.start_experiment(
                strategy_name="ma_crossover",
                config={"a": 1},
                parameters={"p": 2},
                universe_definition={"u": 1},
                feature_definition={"f": 1},
                dataset_hash="abc123",
                experiment_id="exp_test",
            )

            self.assertTrue((record.path / "config.json").exists())
            self.assertTrue((record.path / "metrics.json").exists())
            self.assertTrue((record.path / "artifacts.json").exists())
            self.assertTrue((record.path / "plots").exists())

            registry.finalize_experiment(
                record=record,
                metrics={"sharpe_ratio": 1.23},
                artifacts={"plot": "plots/equity_curve.png"},
                report_text="# done\n",
            )

            metrics_payload = json.loads((record.path / "metrics.json").read_text(encoding="utf-8"))
            self.assertIn("sharpe_ratio", metrics_payload)

            with self.assertRaises(FileExistsError):
                registry.start_experiment(
                    strategy_name="ma_crossover",
                    config={},
                    parameters={},
                    universe_definition={},
                    feature_definition={},
                    dataset_hash="x",
                    experiment_id="exp_test",
                )


if __name__ == "__main__":
    unittest.main()
