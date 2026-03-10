from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from _bootstrap import ROOT  # noqa: F401
from data.loaders.finmind_loader import FinMindLoader


class FinMindLoaderWrapperTests(unittest.TestCase):
    def test_download_price_uses_existing_internal_handler(self) -> None:
        payload = {
            "dataset": "TaiwanStockPrice",
            "table": "price_daily",
            "stock_id": "2330",
            "query_mode": "stock_range",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "db_path": "data/2330.sqlite",
            "fetched_rows": 10,
            "inserted_rows": 9,
            "extra_lines": [],
        }

        with patch("data.loaders.finmind_loader.price.run", return_value=payload) as mock_run:
            loader = FinMindLoader(token="dummy-token")
            result = loader.download_price(
                stock_id="2330",
                start_date="2024-01-01",
                end_date="2024-01-31",
                db_path="data/2330.sqlite",
            )

        self.assertEqual(result.dataset, "TaiwanStockPrice")
        self.assertEqual(result.table, "price_daily")
        self.assertEqual(result.inserted_rows, 9)
        self.assertEqual(result.stock_id, "2330")
        mock_run.assert_called_once()

    def test_subprocess_mode_parses_cli_output(self) -> None:
        stdout = "\n".join(
            [
                "Dataset: TaiwanStockPriceAdj",
                "DB: data/2330.sqlite",
                "Table: price_adj_daily",
                "Fetched rows: 20",
                "Inserted rows: 18",
            ]
        )

        completed = subprocess.CompletedProcess(
            args=["finmind-dl"],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

        with patch("data.loaders.finmind_loader.subprocess.run", return_value=completed) as mock_run:
            loader = FinMindLoader(token="dummy-token", use_subprocess=True)
            result = loader.download_price_adj(
                stock_id="2330",
                start_date="2024-01-01",
                end_date="2024-01-31",
                db_path="data/2330.sqlite",
            )

        self.assertEqual(result.dataset, "TaiwanStockPriceAdj")
        self.assertEqual(result.table, "price_adj_daily")
        self.assertEqual(result.fetched_rows, 20)
        self.assertEqual(result.inserted_rows, 18)
        mock_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
