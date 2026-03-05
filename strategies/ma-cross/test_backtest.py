from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parent / "backtest.py"
SPEC = importlib.util.spec_from_file_location("ma_cross_backtest", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load module from {MODULE_PATH}")
BACKTEST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BACKTEST)


def _create_price_db(db_path: Path, rows: list[tuple[object, ...]], table: str = "price_adj_daily") -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"""
            CREATE TABLE "{table}" (
                date TEXT,
                stock_id TEXT,
                open REAL,
                close REAL,
                is_placeholder INTEGER
            )
            """
        )
        conn.executemany(
            f'INSERT INTO "{table}" (date, stock_id, open, close, is_placeholder) VALUES (?, ?, ?, ?, ?)',
            rows,
        )
        conn.commit()
    finally:
        conn.close()


class BacktestTests(unittest.TestCase):
    def test_load_price_data_filters_placeholder_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            _create_price_db(
                db_path,
                rows=[
                    ("2024-01-02", "2330", 100.0, 100.0, 0),
                    ("2024-01-03", "2330", 101.0, 101.0, 1),
                    ("2024-01-04", "2330", 102.0, None, 0),
                    ("2024-01-05", "2330", 103.0, 103.0, 0),
                ],
            )

            df = BACKTEST.load_price_data(
                db_path=db_path,
                table="price_adj_daily",
                stock_id="2330",
                start_date="2024-01-01",
                end_date="2024-01-31",
            )

            self.assertEqual(len(df), 2)
            self.assertListEqual(
                [d.strftime("%Y-%m-%d") for d in df["date"]],
                ["2024-01-02", "2024-01-05"],
            )

    def test_run_backtest_generates_trades_and_shifted_position(self) -> None:
        close = [10, 10, 10, 11, 12, 13, 12, 11, 10, 9, 8, 9, 10]
        price_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=len(close), freq="D"),
                "stock_id": "2330",
                "open": close,
                "close": close,
            }
        )

        backtest_df, trades_df = BACKTEST.run_backtest(
            price_df=price_df,
            short_window=2,
            long_window=3,
            fee_bps=0.0,
        )

        expected_position = backtest_df["signal"].shift(1).fillna(0).astype(int)
        self.assertTrue((backtest_df["position"] == expected_position).all())
        self.assertGreaterEqual(len(trades_df), 1)

    def test_fee_bps_reduces_final_equity(self) -> None:
        close = [10, 10, 10, 11, 12, 13, 12, 11, 10, 9, 8, 9, 10]
        price_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=len(close), freq="D"),
                "stock_id": "2330",
                "open": close,
                "close": close,
            }
        )

        no_fee_df, _ = BACKTEST.run_backtest(price_df=price_df, short_window=2, long_window=3, fee_bps=0.0)
        fee_df, _ = BACKTEST.run_backtest(price_df=price_df, short_window=2, long_window=3, fee_bps=20.0)

        self.assertLess(float(fee_df["equity"].iloc[-1]), float(no_fee_df["equity"].iloc[-1]))

    def test_open_trade_is_closed_with_eod_reason(self) -> None:
        close = [10, 10, 10, 11, 12, 13, 14, 15, 16]
        price_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=len(close), freq="D"),
                "stock_id": "2330",
                "open": close,
                "close": close,
            }
        )

        _, trades_df = BACKTEST.run_backtest(price_df=price_df, short_window=2, long_window=3, fee_bps=0.0)

        self.assertGreaterEqual(len(trades_df), 1)
        self.assertEqual(str(trades_df.iloc[-1]["exit_reason"]), "eod")

    def test_main_without_ensure_data_does_not_trigger_fetch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            out_dir = Path(tmp) / "out"
            _create_price_db(
                db_path,
                rows=[
                    ("2024-01-01", "2330", 10.0, 10.0, 0),
                    ("2024-01-02", "2330", 10.0, 10.0, 0),
                    ("2024-01-03", "2330", 11.0, 11.0, 0),
                    ("2024-01-04", "2330", 12.0, 12.0, 0),
                    ("2024-01-05", "2330", 13.0, 13.0, 0),
                ],
            )

            with patch.object(
                BACKTEST,
                "ensure_price_data",
                side_effect=AssertionError("ensure_price_data should not be called"),
            ) as mocked:
                code = BACKTEST.main(
                    [
                        "--stock-id",
                        "2330",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2024-01-05",
                        "--short-window",
                        "2",
                        "--long-window",
                        "3",
                        "--db-path",
                        str(db_path),
                        "--output-dir",
                        str(out_dir),
                        "--no-plot",
                    ]
                )

            self.assertEqual(code, 0)
            mocked.assert_not_called()

    def test_missing_table_raises_clear_value_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "2330.sqlite"
            _create_price_db(
                db_path,
                rows=[
                    ("2024-01-01", "2330", 10.0, 10.0, 0),
                ],
                table="price_adj_daily",
            )

            with self.assertRaises(ValueError) as cm:
                BACKTEST.load_price_data(
                    db_path=db_path,
                    table="price_daily",
                    stock_id="2330",
                    start_date="2024-01-01",
                    end_date="2024-01-02",
                )

            message = str(cm.exception)
            self.assertIn("price_daily", message)
            self.assertIn(str(db_path), message)


if __name__ == "__main__":
    unittest.main()
