from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _bootstrap import ROOT  # noqa: F401
from data.loaders.finmind_loader import IngestionResult
from research.data_state import (
    build_data_manifest,
    ensure_local_datasets,
    load_data_catalog,
    resolve_dataset_targets,
)
from finmind_dl.schema import init_schema


def _create_stock_info_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    init_schema(conn)
    conn.execute(
        "INSERT INTO stock_info (date, stock_id, stock_name, type, industry_category) VALUES (?, ?, ?, ?, ?)",
        ("2024-01-01", "2330", "TSMC", "twse", "Semiconductor"),
    )
    conn.execute(
        """
        INSERT INTO meta_runs (
            run_id, dataset, stock_id, query_mode, start_date, end_date,
            requested_params_json, fetched_rows, inserted_rows, status, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "stock_info_run",
            "TaiwanStockInfo",
            "__ALL__",
            "all_market_snapshot",
            "2024-01-01",
            None,
            "{}",
            1,
            1,
            "success",
            None,
        ),
    )
    conn.commit()
    conn.close()


def _create_price_adj_db(db_path: Path, max_date: str = "2024-01-03") -> None:
    conn = sqlite3.connect(db_path)
    init_schema(conn)
    rows = [
        ("2024-01-01", 100.0, 101.0, 99.0, 100.0, 1000, 100000, 0.1, 10, 0),
        ("2024-01-02", 101.0, 102.0, 100.0, 101.0, 1100, 110000, 0.1, 10, 0),
        ("2024-01-03", 102.0, 103.0, 101.0, 102.0, 1200, 120000, 0.1, 10, 0),
    ]
    if max_date >= "2024-01-05":
        rows.extend(
            [
                ("2024-01-04", 103.0, 104.0, 102.0, 103.0, 1300, 130000, 0.1, 10, 0),
                ("2024-01-05", 104.0, 105.0, 103.0, 104.0, 1400, 140000, 0.1, 10, 0),
            ]
        )
    conn.executemany(
        """
        INSERT INTO price_adj_daily (
            date, open, max, min, close, trading_volume,
            trading_money, spread, trading_turnover, is_placeholder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if max_date >= "2024-01-05":
        conn.execute(
            """
            INSERT INTO meta_runs (
                run_id, dataset, stock_id, query_mode, start_date, end_date,
                requested_params_json, fetched_rows, inserted_rows, status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "price_adj_run",
                "TaiwanStockPriceAdj",
                "2330",
                "stock_range",
                "2024-01-04",
                "2024-01-05",
                "{}",
                2,
                2,
                "success",
                None,
            ),
        )
    conn.commit()
    conn.close()


class _FakeLoader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def download_price_adj(self, *, stock_id: str, start_date: str, end_date: str, db_path: str | Path) -> IngestionResult:
        self.calls.append((stock_id, start_date, end_date))
        conn = sqlite3.connect(db_path)
        init_schema(conn)
        conn.executemany(
            """
            INSERT INTO price_adj_daily (
                date, open, max, min, close, trading_volume,
                trading_money, spread, trading_turnover, is_placeholder
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO NOTHING
            """,
            [
                ("2024-01-04", 103.0, 104.0, 102.0, 103.0, 1300, 130000, 0.1, 10, 0),
                ("2024-01-05", 104.0, 105.0, 103.0, 104.0, 1400, 140000, 0.1, 10, 0),
            ],
        )
        conn.execute(
            """
            INSERT INTO meta_runs (
                run_id, dataset, stock_id, query_mode, start_date, end_date,
                requested_params_json, fetched_rows, inserted_rows, status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "fake_ingest_run",
                "TaiwanStockPriceAdj",
                stock_id,
                "stock_range",
                start_date,
                end_date,
                "{}",
                2,
                2,
                "success",
                None,
            ),
        )
        conn.commit()
        conn.close()
        return IngestionResult(
            dataset="TaiwanStockPriceAdj",
            table="price_adj_daily",
            stock_id=stock_id,
            query_mode="stock_range",
            start_date=start_date,
            end_date=end_date,
            db_path=Path(db_path),
            fetched_rows=2,
            inserted_rows=2,
            extra_lines=[],
        )


class ResearchDataStateTests(unittest.TestCase):
    def test_ensure_local_datasets_only_ingests_when_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            data_root.mkdir(parents=True, exist_ok=True)

            _create_stock_info_db(data_root / "market.sqlite")
            _create_price_adj_db(data_root / "2330.sqlite", max_date="2024-01-03")

            catalog = load_data_catalog(ROOT / "data" / "catalog" / "data_catalog.yaml")
            targets = resolve_dataset_targets(
                required_datasets=["price_adj"],
                stock_ids=["2330"],
                data_root=data_root,
                catalog=catalog,
            )

            loader = _FakeLoader()
            ensure_local_datasets(
                targets=targets,
                analysis_start="2024-01-01",
                data_as_of="2024-01-05",
                data_update_policy={"auto_update_missing": True, "auto_update_stale": True, "stock_info_start_date": "2024-01-01"},
                finmind_loader=loader,  # type: ignore[arg-type]
            )
            ensure_local_datasets(
                targets=targets,
                analysis_start="2024-01-01",
                data_as_of="2024-01-05",
                data_update_policy={"auto_update_missing": True, "auto_update_stale": True, "stock_info_start_date": "2024-01-01"},
                finmind_loader=loader,  # type: ignore[arg-type]
            )

            self.assertEqual(loader.calls, [("2330", "2024-01-04", "2024-01-05")])

    def test_data_manifest_captures_row_counts_and_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            root = Path(tmp_name)
            data_root = root / "data"
            data_root.mkdir(parents=True, exist_ok=True)

            _create_stock_info_db(data_root / "market.sqlite")
            _create_price_adj_db(data_root / "2330.sqlite", max_date="2024-01-05")

            catalog = load_data_catalog(ROOT / "data" / "catalog" / "data_catalog.yaml")
            targets = resolve_dataset_targets(
                required_datasets=["stock_info", "price_adj"],
                stock_ids=["2330"],
                data_root=data_root,
                catalog=catalog,
            )
            manifest = build_data_manifest(
                research_id="ma_cross_example_v1",
                run_id="run_test",
                data_as_of="2024-01-05",
                analysis_start="2024-01-01",
                targets=targets,
            )

            self.assertEqual(manifest["research_id"], "ma_cross_example_v1")
            self.assertTrue(manifest["dataset_fingerprint"])
            self.assertEqual(len(manifest["datasets"]), 2)
            summary = {item["dataset_name"]: item for item in manifest["dataset_summary"]}
            self.assertEqual(summary["price_adj"]["row_count_as_of_total"], 5)
            self.assertEqual(summary["price_adj"]["max_date"], "2024-01-05")


if __name__ == "__main__":
    unittest.main()
