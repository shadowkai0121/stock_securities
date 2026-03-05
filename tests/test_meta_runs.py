from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.core.history import try_log_meta_run


class MetaRunsTests(unittest.TestCase):
    def test_success_and_error_rows_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "meta.sqlite"

            try_log_meta_run(
                db_path,
                run_id="run_success",
                dataset="TaiwanStockPrice",
                stock_id="2330",
                query_mode="stock_range",
                start_date="2026-03-01",
                end_date="2026-03-02",
                requested_params_json="{}",
                fetched_rows=2,
                inserted_rows=2,
                status="success",
                error_message=None,
            )
            try_log_meta_run(
                db_path,
                run_id="run_error",
                dataset="TaiwanStockPrice",
                stock_id="2330",
                query_mode="stock_range",
                start_date="2026-03-01",
                end_date="2026-03-02",
                requested_params_json="{}",
                fetched_rows=0,
                inserted_rows=0,
                status="error",
                error_message="boom",
            )

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT run_id, status, error_message FROM meta_runs ORDER BY run_id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][0], "run_error")
            self.assertEqual(rows[0][1], "error")
            self.assertEqual(rows[1][1], "success")


if __name__ == "__main__":
    unittest.main()
