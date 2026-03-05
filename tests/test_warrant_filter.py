from __future__ import annotations

import unittest
from datetime import date

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.datasets.warrant import filter_active_rows


class WarrantFilterTests(unittest.TestCase):
    def test_filter_active_rows(self) -> None:
        rows = [
            {"end_date": "2026-03-10"},
            {"end_date": "2026-02-01"},
            {"end_date": ""},
        ]
        filtered = filter_active_rows(rows, ref_date=date(2026, 3, 5))
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["end_date"], "2026-03-10")


if __name__ == "__main__":
    unittest.main()
