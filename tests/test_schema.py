from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from finmind_dl.schema import init_schema


class SchemaTests(unittest.TestCase):
    def test_init_schema_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                init_schema(conn)
                init_schema(conn)
                conn.commit()
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            finally:
                conn.close()

        self.assertIn("meta_runs", tables)
        self.assertIn("price_daily", tables)
        self.assertIn("broker_trades", tables)


if __name__ == "__main__":
    unittest.main()
