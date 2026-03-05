from __future__ import annotations

import unittest

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.cli import main


class CliValidationTests(unittest.TestCase):
    def test_argparse_failure_returns_2(self) -> None:
        code = main(["price"])
        self.assertEqual(code, 2)

    def test_invalid_date_returns_2(self) -> None:
        code = main(
            [
                "price",
                "--stock-id",
                "2330",
                "--start-date",
                "2026-13-01",
                "--end-date",
                "2026-03-01",
                "--token",
                "dummy",
            ]
        )
        self.assertEqual(code, 2)

    def test_holding_mode_conflict_returns_2(self) -> None:
        code = main(
            [
                "holding-shares",
                "--stock-id",
                "2330",
                "--all-market-date",
                "2026-03-01",
                "--token",
                "dummy",
            ]
        )
        self.assertEqual(code, 2)

    def test_daily_invalid_range_returns_2(self) -> None:
        code = main(
            [
                "daily",
                "--stock-id",
                "2330",
                "--start-date",
                "2026-03-05",
                "--end-date",
                "2026-03-01",
                "--token",
                "dummy",
            ]
        )
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
