from __future__ import annotations

import json
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401


class DataCatalogIntegrityTests(unittest.TestCase):
    def test_catalog_has_required_datasets_and_fields(self) -> None:
        path = ROOT / "data" / "catalog" / "data_catalog.yaml"
        self.assertTrue(path.exists(), "data catalog file should exist")

        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertIn("datasets", payload)

        datasets = payload["datasets"]
        self.assertIsInstance(datasets, list)
        self.assertGreater(len(datasets), 0)

        required_names = {
            "price",
            "price_adj",
            "margin",
            "broker",
            "holding_shares",
            "stock_info",
            "warrant",
        }
        names = {str(item.get("dataset_name")) for item in datasets}
        self.assertTrue(required_names.issubset(names))

        required_fields = {
            "dataset_name",
            "source",
            "ingestion_command",
            "storage_table",
            "primary_keys",
            "grain",
            "frequency",
            "description",
            "required_for",
            "quality_checks",
        }
        for item in datasets:
            self.assertTrue(required_fields.issubset(set(item.keys())))


if __name__ == "__main__":
    unittest.main()
